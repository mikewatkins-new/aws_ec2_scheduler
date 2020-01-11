from boto3 import client
import botocore.exceptions
import automated.exceptions
import logging
import automated.ec2_actions as ec2_actions
import config

logger = logging.getLogger()


class EC2:

    def __init__(self, region: str, ec2_conn: str = config.EC2_CONN_DEFAULT):
        """
        :param region: str - AWS region for connection endpoint
        :param ec2_conn: str - EC2 connection endpoint. Used for mock testing methods
        """
        self._region = region
        self.__ec2_conn = ec2_conn
        self.__ec2 = client("ec2", region_name=self._region)
        self._testing = config.is_test_run()
        self.__errors = []

    @property
    def errors(self):
        return self.__errors

    @errors.setter
    def errors(self, error_message):
        self.__errors.append(error_message)

    @errors.getter
    def errors(self):
        return self.__errors

    def get_instances_from_tag_key(self, tag_key: str) -> list:
        """
        Gets instance id's from EC2 instances tagged with AWS automated scheduler defined automation tag only
        :param tag_key: str = The tag key to filter ec2 results on
        :return list = collection of instances and associated tag values retrieved from them
        """
        instance_id_list: list = []

        # TESTING: simulated ec2 instances returned with data structure as real
        if self._testing:
            return self.__testing_return_mock_ec2_instances()

        ec2_paginator = self.__ec2.get_paginator('describe_instances')

        # This should be an iterable object that it returns that we can iterate over on subsequent calls
        ec2_iterator = None

        logger.info(f"Looking for instances with tag: '{tag_key}'")
        # Only return instances that have the automation scheduling tag in them
        try:
            ec2_iterator = ec2_paginator.paginate(
                Filters=[
                    {
                        "Name": "tag-key",
                        "Values": [tag_key]
                    }
                ]
            )

        except botocore.exceptions.ParamValidationError as err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"AWS Tag key in improper format or missing: {err}",
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )

        # TODO Test the iterator so we can actually handle instances over the single retrieval cap
        # Logging this because I want to see what happens when we exceed the first batch of return instances
        # So I can add proper pagination logic
        logger.info(f"Paginator response: {ec2_iterator.build_full_result()}")
        logger.info(f"Paginator response: {ec2_iterator.result_key_iters()}")

        # TODO: Might consider using the following to give me full ec2 response for contract testing
        # import json
        # json_format = json.dumps(ec2_iterator.build_full_result(), indent=2, sort_keys=True, default=str)

        # TODO Add pagination logic, and discover proper override tag usage
        # Drill down to get the instance id, the value of the schedule tag and check override tag
        for results in ec2_iterator:
            for reservations in results['Reservations']:
                for instance in reservations['Instances']:
                    tag_value = self.get_tag_value(instance['InstanceId'], tag_key)
                    override = self.get_tag_value(instance['InstanceId'], "override")

                    inst = {
                        "instance_id": instance['InstanceId'],
                        "tag": tag_value,
                        "override": override
                    }

                    instance_id_list.append(inst)
                    logger.info(f"Discovered EC2 resource: {inst}")

        return instance_id_list

    def get_tag_value(self, resource_id: str, tag_key: str) -> str:
        """
        Gets the value of the specified tag from the specified resource
        :param str resource_id: AWS ID of the resource
        :param str tag_key: Tag key to get the value of
        :return str tag_value: Value of tag retrieved from tag key
        """

        tag_value = None

        # TODO: Give this one a shot. If we can get this to work, we can make queries independent of
        # describe action, so we can reuse this function, just passing a resource type and resource id
        query = {
            "Filters": [
                {
                    "Name": "resource-id",
                    "Values": [resource_id]
                }
            ]
        }

        # It seems that boto3.resource.ec2.describe_tags can not use multiple filters or I would filter on the
        # instance and the tag itself. Instead, filter of the instance id, then retrieve the tag from it
        tags = self.__ec2.describe_tags(**query)
        #     Filters=[
        #         {
        #             "Name": "resource-id",
        #             "Values": [resource_id]
        #         }
        #     ]
        # )

        # Find our scheduling tag and get the value of the [key: value] pair
        # Since we only filters on instance type, we have to loop through all the tags to find the one we want
        for tag in tags['Tags']:
            if tag.get('Key') == tag_key:
                tag_value = tag.get('Value')

        return tag_value

    def perform_action(self, action: str, instance_id: str) -> dict:
        """
        :param action:
        :param instance_id:
        """
        logger.info(f"Received call to perform action [{action}] on [{instance_id}]")

        response = None

        if action is ec2_actions.START:
            logger.info(f"Starting instance [{instance_id}]...")
            response = self.__start_instance(instance_id)
        elif action is ec2_actions.STOP:
            logger.info(f"Stopping instance [{instance_id}]...")
            response = self.__stop_instance(instance_id)

        state_changed = self.__was_instances_state_changed(action, response)

        return state_changed

    # TODO: Ran into this situation
    # 2020-01-11 22:47:52,454 [ERROR] Problem starting instance i-00a9e45169fcb0651: An error occurred (IncorrectInstanceState)
    # when calling the StartInstances operation: The instance 'i-00a9e45169fcb0651' is not in a state from which it can be started.
    # I want to log the error, but not cause a fatal response

    def __start_instance(self, instance_id: str):
        """
        :param instance_id:
        :return:
        """
        response = None

        try:
            response = self.__ec2.start_instances(
                InstanceIds=[
                    instance_id
                ],
                DryRun=False
            )
        except botocore.exceptions.ClientError as err:
            if err.response.get('Error').get('Code') == "UnauthorizedOperation":
                logger.warning(err)
            elif err.response.get('Error').get('Code') == "InvalidInstanceID.NotFound":
                logger.error(f"Unable to perform action <stop> on {instance_id} - Unable to find the instance.")
            else:
                logger.error(f"Problem starting instance {instance_id}: {err}")
                raise automated.exceptions.ClientError(err)

        return response

    def __stop_instance(self, instance_id: str):
        """
        Look into enabling hibernation for instances, but probably not a use case for this app
        :param instance_id:
        :return:
        """
        # This could become a single call for stopped instances if we want by building up a list of actions.stop
        # This does not throw an error if the instance is already stopped, pending start, or pending stop
        # and we try and stop it. It might be worthwhile to just make a single stop call rather than check the state

        response = None
        try:
            response = self.__ec2.stop_instances(
                InstanceIds=[
                    instance_id
                ],
                Hibernate=False,
                DryRun=False
            )
        except botocore.exceptions.ClientError as err:
            if err.response.get('Error').get('Code') == "UnauthorizedOperation":
                logger.warning(err)
            elif err.response.get('Error').get('Code') == "InvalidInstanceID.NotFound":
                logger.error(f"Unable to perform action <stop> on {instance_id} - Unable to find the instance.")
            else:
                raise automated.exceptions.ClientError(err)

        return response

    def __was_instances_state_changed(self, action, instance_api_response):
        # Assumption that we are only stopping one instance at a time, if we end up starting/stopping
        # multiple then we will have to rework the logic to return the status code along with the instance
        # or change the logic otherwise to receive the single response and return result
        if action == ec2_actions.STOP:
            current_state = instance_api_response.get('StoppingInstances')[0].get('CurrentState')
            previous_state = instance_api_response.get('StoppingInstances')[0].get('PreviousState')
        elif action == ec2_actions.START:
            current_state = instance_api_response.get('StartingInstances')[0].get('CurrentState')
            previous_state = instance_api_response.get('StartingInstances')[0].get('PreviousState')

        if current_state != previous_state:
            logger.info(f"Instance changed states from {previous_state} to {current_state}.")
        else:
            logger.info(f"Instance desired state {action} was already in state {previous_state['Name']}")

        return current_state != previous_state

    # TESTING ONLY
    def __testing_return_mock_ec2_instances(self) -> list:
        return [
            {"instance_id": "i-007", "tag": "us_hours", "override": None},
            {"instance_id": "i-0049", "tag": "uk_hours", "override": None},
            {"instance_id": "i-512", "tag": "austin_offices", "override": None},
            {"instance_id": "i-212", "tag": "new_york_offices", "override": None},
        ]
