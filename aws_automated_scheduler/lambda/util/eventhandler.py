import config
import events.type
import events.scheduler
import events.put_config
import events.http_response as http_response
import logging
import events.retrieve_config
import os
import automated.exceptions

logger = logging.getLogger()


class AutomationEventHandler:
    """
    Performs appropriate action on event received from Lambda
    """

    def __init__(self, event: dict, context: dict):
        self.__event: dict = event
        self.__context: dict = context
        self.__errors: list = []
        self._test_run: bool = config.is_test_run()

    @property
    def errors(self):
        return self.__errors

    @errors.getter
    def errors(self):
        return self.__errors

    @errors.setter
    def errors(self, error_message):
        self.__errors.append(error_message)

    def evaluate_event(self) -> dict:
        """
        Evaluates the received event and performs appropriate action based on the event
        Possible events are defined in events.types module and currently are:
            * CW_SCHEDULED_EVENT: Scheduled event that is the basis of the app functionality
            * API_S3_PUT_CONFIG: CloudTrail API event that is sent when config is updated
            * API_RETRIEVE_DYNAMO_AS_CONFIG: API triggered event that retrieves all items from Automated DynamoDB Table
        :return: dict = HTTP response to return to caller
        """

        logger.info(f"Received event: '{self.__event}'")
        logger.info(f"Received context: '{self.__context}'")

        response: dict = dict()
        event_type: str = self.__event["detail-type"]

        if not self._test_run:
            try:
                if event_type == "Scheduled Event":
                    event_type = events.type.CW_SCHEDULED_EVENT

                elif event_type == "AWS API Call via CloudTrail" \
                        and self.__event["detail"]["eventName"] == "PutObject":
                    event_type = events.type.API_S3_PUT_CONFIG

            except KeyError:
                automated.exceptions.log_error(
                    self,
                    error_message=f"Event type '{event_type}' unknown.",
                    include_in_http_response=True,
                    http_status_code=http_response.NOT_FOUND,
                    output_to_logger=True,
                    fatal_error=True
                )

        if event_type == events.type.CW_SCHEDULED_EVENT:
            scheduler = events.scheduler.Scheduler(self._get_env_variables())
            response = scheduler.automated_schedule()

        elif event_type == events.type.API_S3_PUT_CONFIG:
            response = events.put_config.put_config_into_dynamo(self._get_env_variables())

        elif event_type == events.type.API_RETRIEVE_DYNAMO_AS_CONFIG:
            response = events.retrieve_config.retrieve_dynamo_as_config(self._get_env_variables())

        else:
            automated.exceptions.log_error(
                self,
                error_message=f"Event type '{event_type}' not found.",
                include_in_http_response=True,
                http_status_code=http_response.NOT_FOUND,
                output_to_logger=True,
                fatal_error=True
            )

        return response

    def _get_env_variables(self) -> dict:
        """
        Returns Lambda environment variables common to majority of application functionality.
        These variables are defined/expected by AWS CDK synth/deploy
        :return: dict: Environment variables retrieved from Lambda environment
        """

        try:
            region = os.environ["scheduler_region"]
            logger.debug(f"Discovered dynamodb region name [{region}] from environment variable 'scheduler_region'")
        except KeyError:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Unable to discover region name from environment variable 'scheduler_region'",
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )
        try:
            tag_key = os.environ['scheduler_tag']
            logger.debug(f"Discovered tag key [{tag_key}] from environment variable 'scheduler_tag'")
        except KeyError:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Unable to discover scheduler tag key name from environment variable 'scheduler_tag'",
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )
        try:
            table_name = os.environ['scheduler_table']
            logger.debug(f"Discovered dynamodb table name [{table_name}] from environment variable 'scheduler_table'")
        except KeyError:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Unable to discover scheduler table name from environment variable 'scheduler_table'",
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )

        return {
            "region": region,
            "tag_key": tag_key,
            "table_name": table_name
        }
