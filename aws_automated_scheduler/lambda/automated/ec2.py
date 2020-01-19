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
        self._test_run = config.is_test_run()
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
        Retrieve instance id's from EC2 instances tagged with AWS automated scheduler defined automation tag only
        :param tag_key: str = The tag key to filter ec2 results on
        :return list = collection of instances and associated tag values retrieved from them
        """

        if not self._test_run:
            paginated_instances = self._retrieve_all_instances_with_tag_key(tag_key=tag_key)
            instance_id_list: list = self._retrieve_instance_id_and_schedule_tag_info(
                paginated_instances=paginated_instances,
                tag_key=tag_key
            )
        else:
            instance_id_list: list = self.__testing_get_mock_ec2_instances()

        return instance_id_list

    def _retrieve_all_instances_with_tag_key(self, tag_key: str):
        ec2_paginator = self.__ec2.get_paginator('describe_instances')
        ec2_iterator = None

        logger.info(f"Looking for all instances in '{self._region} with tag: '{tag_key}'")

        query_filter = {
            "Filters": [
                {
                    "Name": "tag-key",
                    "Values": [tag_key]
                }
            ]
        }

        try:
            ec2_iterator = ec2_paginator.paginate(**query_filter)

        except botocore.exceptions.ParamValidationError as err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"AWS Tag key in improper format or missing: {err}",
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )

        return ec2_iterator

    def _retrieve_instance_id_and_schedule_tag_info(self, paginated_instances, tag_key) -> list:
        instance_id_list: list = []

        # CODE REVIEW: Is there a more performant way to do this? Each iteration is a little slow and would cause
        # a linear decrease in speed with n number of instances. Is it because we are calling describe tags twice
        # for each instance?

        for results in paginated_instances:
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

        query = {
            "Filters": [
                {
                    "Name": "resource-id",
                    "Values": [resource_id]
                }
            ]
        }

        # It seems that boto3.resource.ec2.describe_tags can not use multiple filters or I would filter on the
        # instance and the tag itself. Instead, filter off the instance id, then retrieve the tag from it
        tags = self.__ec2.describe_tags(**query)

        # Find our scheduling tag and get the value of the [key: value] pair
        # Since we only filters on instance type, we have to loop through all the tags to find the one we want
        for tag in tags['Tags']:
            if tag.get('Key') == tag_key:
                tag_value = tag.get('Value')

        return tag_value

    def get_tag_value_v2(self, resource_id: str, tag_keys: set) -> set:
        """
        Attempt to speed up calls to retrieve tags. Instead of one call for each tag, make it get all tags for one call
        """

        tag_value = None

        query = {
            "Filters": [
                {
                    "Name": "resource-id",
                    "Values": [resource_id]
                }
            ]
        }

        # It seems that boto3.resource.ec2.describe_tags can not use multiple filters or I would filter on the
        # instance and the tag itself. Instead, filter off the instance id, then retrieve the tag from it
        tags = self.__ec2.describe_tags(**query)

        schedule_value = None
        override_value = None

        # Find our scheduling tag and get the value of the [key: value] pair
        # Since we only filters on instance type, we have to loop through all the tags to find the one we want
        for tag in tags['Tags']:
            if tag.get('Key') in tag_keys:
                if tag.get('Key') == "Schedule":
                    schedule_value = tag.get('Value')
                elif tag.get('Key') == "override":
                    override_value = tag.get('Value')

        return set(resource_id, schedule_value, override_value)

    def perform_action(self, action: str, instance_id: str) -> bool:
        """
        :param action:
        :param instance_id:
        """
        logger.info(f"Received call to perform action [{action}] on [{instance_id}]")

        response = None
        state_changed = False

        if action is ec2_actions.START:
            logger.info(f"Starting instance [{instance_id}]...")
            response = self.__start_instance(instance_id)
        elif action is ec2_actions.STOP:
            logger.info(f"Stopping instance [{instance_id}]...")
            response = self.__stop_instance(instance_id)

        if response is not None:
            state_changed = self.__was_instances_state_changed(action, response)

        return state_changed

    # TODO: Ran into this situation
    # [ERROR] Problem starting instance i-00a9e45169fcb0651: An error occurred (IncorrectInstanceState) when calling
    # the StartInstances operation: The instance 'i-00a9e45169fcb0651' is not in a state from which it can be started.
    # I want to log the error, but not cause a fatal response or exception

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
    def __testing_get_mock_ec2_instances(self) -> list:
        return [
            {"instance_id": "i-007", "tag": "us_hours", "override": None},
            {"instance_id": "i-0049", "tag": "uk_hours", "override": None},
            {"instance_id": "i-512", "tag": "austin_offices", "override": None},
            {"instance_id": "i-212", "tag": "new_york_offices", "override": None},
        ]
