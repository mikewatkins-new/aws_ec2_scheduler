import automated.ec2
import automated.dynamodb
import events.http_response as http_response
import util.evalperiod
import util.eventhandler
import logging
import automated.exceptions
import automated.ec2_actions as ec2_actions
import config
import events.type

logger = logging.getLogger()


class Scheduler:
    """
    Main happy path for scheduling operations
    """

    def __init__(self, env_vars):
        self._region: str = env_vars.get("region")
        self._tag_key: str = env_vars.get("tag_key")
        self._table_name: str = env_vars.get("table_name")
        self._test_run: bool = config.is_test_run()
        self.__errors: list = []

        if self._test_run:
            self.__ec2: automated.ec2.EC2 = automated.ec2.EC2(
                region=self._region,
                ec2_conn=config.EC2_CONN_LOCAL
            )
            self.__dynamo_db = automated.dynamodb.DynamoDB(
                region=self._region,
                table_name=self._table_name,
                db_conn=config.DB_CONN_LOCAL)

        elif not self._test_run:
            self.__ec2 = automated.ec2.EC2(
                region=self._region,
                ec2_conn=config.EC2_CONN_DEFAULT
            )
            self.__dynamo_db = automated.dynamodb.DynamoDB(
                region=self._region,
                table_name=self._table_name,
                db_conn=config.DB_CONN_SERVERLESS
            )

        self.__evaluator = util.evalperiod.EvalPeriod()

    @property
    def errors(self):
        return self.__errors

    @errors.getter
    def errors(self):
        return self.__errors

    @errors.setter
    def errors(self, error_message):
        self.__errors.append(error_message)

    def automated_schedule(self) -> dict:
        """
        Start of path for scheduling operations.
        Search for EC2 instances that have our automation tag applied to them.
        Retrieve the instance id and tag value as the schedule value for that instance.
        Query DynamoDB with the schedule tag value to find all the periods (days/hours/start and stop times) assigned
        Check if any of those periods are a match for the current day/hour/time.
        Perform the appropriate scheduling action if a match
        :return: dict = http response with results of operation
        """

        instance_list: list = self.__ec2.get_instances_from_tag_key(self._tag_key)
        logger.info(f"Found [{len(instance_list)}] instances with tag [{self._tag_key}] in [{self._region}]")

        modified_instances: list = []

        if instance_list:
            evaluator = util.evalperiod.EvalPeriod()
            modified_instances = self.__evaluate_instances(instance_list, evaluator)

        logger.info(f"No more instances found in region [{self._region}] with tag name [{self._tag_key}]")
        logger.info(f"--------------------------------")
        logger.info(f"Final results of instances that changed state:\n{modified_instances}")

        found_errors = self.retrieve_errors_from_components()
        if found_errors:
            # TODO: Consider different HTTP response code for multiple errors
            response = http_response.construct_http_response(http_response.INTERNAL_ERROR, found_errors)
        else:
            # TODO: Add better JSON response indicating how many actions were performed
            response = http_response.construct_http_response(
                status_code=http_response.OK,
                message=[f"Success from event: '{events.type.CW_SCHEDULED_EVENT}'",
                         f"Modified instances: {modified_instances}"]
            )

        return response

    def __evaluate_instances(self, instance_list: list, evaluator: util.evalperiod.EvalPeriod) -> list:
        """
        Checks list of instances to see if any of the returned schedule tag values should be evaluated
        Overview:
            - Instance has automation scheduler tag: e.g. ['Schedule', 'austin_hours']
            - DynamoDB is queried to see what periods 'austin_hours' schedule item contains
            - 'austin_hours' returns periods ['MON-FRI-START-0800-STOP-1800', 'SAT-START-1000-STOP-1400']
            - DynamoDB is queried to retrieve information on the each associated period
            - Evaluate if any of those periods are a match for the current day/hour/time.
            - Perform the appropriate scheduling(start/stop) action if a match
        :param instance_list: list = instance list with element structure {"instance_id": "foo", "tag": "bar"}
        :param evaluator:
        :return:
        """

        # TODO break this function up into multiple functions

        instances_with_changed_status: list = []

        for index, instance in enumerate(instance_list, start=1):

            instance_id: str = instance['instance_id']
            tag_value: str = instance['tag']
            # Override is not a required tag
            override: str = instance.get('override', None)
            logger.info(f"Evaluating instance [{index} of {len(instance_list)}]...")
            logger.info(
                f"Instance Id: [{instance_id}] with schedule tag value: [{tag_value}] and override action: [{override}]"
            )

            schedule_info: list = self.__dynamo_db.retrieve_schedule_info(schedule_name=tag_value)

            # timezone is not implement yet
            periods, timezone = self._retrieve_period_info_from_schedule(schedule_info=schedule_info)

            action_type = ec2_actions.NONE

            period_info: list = []
            if len(periods):
                logger.info(
                    f"Using sk: [{tag_value}], retrieved [{len(periods)}] values from periods attribute - {periods}"
                )

                period_info = self.__dynamo_db.retrieve_period_info(periods)

                if len(period_info):
                    for period in period_info:
                        action_type = evaluator.eval_period(period)

                        if action_type is not ec2_actions.NONE:
                            # We have found an action, stop evaluating the remaining periods, no conflicting actions.
                            logger.info(f"Found [{action_type}] action! Breaking check for remaining periods...")
                            break
                else:
                    logger.info(
                        f"No period information was gathered from: [{periods}]. Do these items exist in the db?")
            else:
                logger.warning(f"Unable to retrieve period info from empty period list: {periods}")

            logger.info(f"Received action type '{action_type}' for '{instance_id}'")

            if self._test_run:
                logger.info(f"Using local (not real) EC2: Performing action type '{action_type}' on '{instance_id}'")
            else:
                instance_changed = self.__ec2.perform_action(action_type, instance_id)
                if instance_changed:
                    instances_with_changed_status.append((instance_id, action_type))

            logger.info(f"Finished evaluating InstanceId: '{instance_id}', [{len(instance_list) - index}] remaining.")
            logger.info(f"--------------------------------")

        return instances_with_changed_status

    def _retrieve_period_info_from_schedule(self, schedule_info: list) -> tuple:
        """
        We are only retrieving a single schedule item from DynamoDB. If this is ever changed to batch get
        all the scheduled items, then the following logic will need to be modified to handle multiple items
        Periods are retrieved as a set from DynamoDB JSON response
        :param schedule_info:
        :return: tuple = (list = list of periods, str = timezone)
        """
        periods, timezone = None, None
        if len(schedule_info):
            periods = schedule_info[0].get('periods')
            timezone = schedule_info[0].get('timezone')

        # Convert periods retrieved from set to list
        if periods:
            periods = list(periods)

        return periods, timezone

    def retrieve_errors_from_components(self):
        """
        Checks automation components for errors to compile them into single http response
        :return: list = collection of logged errors from automation components
        """
        found_errors = []
        if self.__dynamo_db.errors:
            logger.info(f"Checking for errors from DynamoDB... Found {len(self.__dynamo_db.errors)} error(s).")
            found_errors.extend(self.__dynamo_db.errors)

        if self.__evaluator.errors:
            logger.info(f"Checking for errors from Period Evaluation... Found {len(self.__evaluator.errors)} error(s).")
            found_errors.extend(self.__evaluator.errors)

        if self.__ec2.errors:
            logger.info(f"Checking for errors from EC2... Found {len(self.__ec2.errors)} error(s).")
            found_errors.extend(self.__ec2.errors)

        return found_errors
