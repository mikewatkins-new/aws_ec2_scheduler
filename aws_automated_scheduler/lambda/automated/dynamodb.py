from util import data
import automated.exceptions
import automated.s3
from boto3 import client
import botocore.exceptions
import botocore.errorfactory
import logging
import config

logger = logging.getLogger()

# NOTE: Had to set environment variable TZ=UTC in order for dynamodb describe table and create_table due to boto3 bug
# for Windows.


class DynamoDB:
    """Retrieve scheduling data from DynamoDB"""

    def __init__(self, region, table_name, db_conn=config.DB_CONN_SERVERLESS):
        self.__region = region
        self.__table_name = table_name
        self.__testing = config.is_test_run()
        self.__errors = []

        if self.__testing:
            logger.warning("[TESTING] Using local database connection.")
            self.dynamodb = client(
                "dynamodb",
                region_name=self.__region,
                endpoint_url=config.DB_CONN_LOCAL_ENDPOINT
            )
            self.__testing_create_table()

        else:
            self.dynamodb = client(
                "dynamodb",
                region_name=self.__region,
            )

    @property
    def errors(self):
        return self.__errors

    @errors.setter
    def errors(self, error_message):
        self.__errors.append(error_message)

    @errors.getter
    def errors(self):
        return self.__errors

    # TESTING ONLY
    def __testing_create_table(self):
        """
        Create table should be called from CDK in normal operations
        This is only for DynamoDB local connection testing when it is not created on deploy
        # BUG: See note above about environment variable TZ being set to UTC on Windows or failure
        """
        logger.info(f"Checking DynamoDB to see if table [{self.__table_name}] exists...")
        table_exists = self.__testing_check_dynamo_db_table_existence()

        # If table does not exist, create it
        if not table_exists:
            logger.info(f"Creating table [{self.__table_name}]...")

            try:
                response = self.dynamodb.create_table(
                    TableName=self.__table_name,
                    AttributeDefinitions=[
                        {
                            "AttributeName": "pk",
                            "AttributeType": "S"
                        },
                        {
                            "AttributeName": "sk",
                            "AttributeType": "S"
                        }
                    ],
                    KeySchema=[
                        {
                            "AttributeName": "pk",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "sk",
                            "KeyType": "RANGE"
                        }
                    ],
                    ProvisionedThroughput={
                        'ReadCapacityUnits': 5,
                        'WriteCapacityUnits': 5,
                    }
                )

                logger.info(f"Table [{self.__table_name}] successfully created.")
                logger.debug(f"Table creation response: {response}")

            except botocore.exceptions.ClientError as boto_err:
                automated.exceptions.log_error(
                    automation_component=self,
                    error_message=f"Problem creating DynamoDB table: {boto_err}",
                    output_to_logger=True,
                    include_in_http_response=True,
                    http_status_code=boto_err.response.get("ResponseMetadata").get("HTTPStatusCode"),
                    fatal_error=True
                )

        else:
            logger.info(f"Table [{self.__table_name}] already exists. Skipping creation.")

    # TESTING ONLY
    def __testing_check_dynamo_db_table_existence(self):
        table_exists = False
        try:
            self.dynamodb.describe_table(
                TableName=self.__table_name
            )
            # If we do not throw an exception for non-existent table, set to to true
            table_exists = True

        except botocore.exceptions.ClientError:
            # Exception pops out if table does not exist. Do not want to fail, just pass the info along
            logger.info(f"Table [{self.__table_name}] does not exist.")
        except botocore.exceptions.EndpointConnectionError as boto_err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Connection error: {boto_err}",
                output_to_logger=True,
                http_status_code=None,
                fatal_error=True
            )

        return table_exists

    def retrieve_all_items_from_dynamo(self) -> list:
        """
        Retrieves all items and attributes from Automated Scheduler table
        :return: list = list of DynamoDB items
        """

        response = None
        logger.info(f"Attempting to retrieve all items from DynamoDB...")

        try:
            response = self.dynamodb.scan(
                TableName=self.__table_name
            )
        except botocore.exceptions.ClientError as err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Error retrieving all items from DynamoDB: {err}",
                output_to_logger=True,
                include_in_http_response=True,
                http_status_code=err.response.get('ResponseMetadata').get('HTTPStatusCode'),
                fatal_error=True
            )
            logger.error()

        # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.scan
        if "LastEvaluatedKey" in response:
            logger.error(
                "You have not finished writing the code to retrieve over 1MB of data. Shame shame."
                "There should never be a chance that we exceed 1MB of data in our simple request, so... what happened?")

        logger.info(f"Retrieved [{len(response['Items'])}] items from DynamoDB.")
        return response['Items']

    def load_json_into_db(self, json_data: list) -> dict:
        """
        Loads DynamoDB compatible JSON into DynamoDB table as batch job
        :param json_data: DynamoDB batch write compatible JSON
        :return:
        """

        # TODO: Does batch write have a limit?

        logger.info(f"Attempting to load [{len(json_data)}] items into DynamoDB table [{self.__table_name}]")
        batch_write_compatible_json = []
        response = None

        # Iterate through list, and prepend the required {"PutRequest": "Item" { ... } for DynamoDB batch write
        for item in json_data:
            request = {
                "PutRequest": {
                    "Item": item
                }
            }
            batch_write_compatible_json.append(request)

        try:
            response = self.dynamodb.batch_write_item(
                RequestItems={
                    self.__table_name: batch_write_compatible_json
                }
            )
        except botocore.exceptions.ParamValidationError as err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"DynamoDB validation failed: {err}",
                output_to_logger=True,
                fatal_error=False
            )
        except botocore.exceptions.ClientError as err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"DynamoDB client error: {err}",
                output_to_logger=True,
                http_status_code=err.response.get('ResponseMetadata').get('HTTPStatusCode'),
                fatal_error=True
            )

        return response

    # # TODO: Is this only for testing now? Can probably move this to testing.
    # def put_period_item(self, sort_key, days_of_week=None, start_time=None, stop_time=None):
    #     """
    #     :return:
    #     """
    #
    #     # Attributes we require for putting an item
    #     request_item = {
    #         "pk": {
    #             "S": "period"
    #         },
    #         "sk": {
    #             "S": sort_key
    #         }
    #     }
    #
    #     # Is this bad practice? Does it slow things down unnecessarily?
    #     # Perhaps I can start with all attributes and then .pop remove the ones I don't want for speed
    #     if days_of_week:
    #         request_item = {**request_item, "days_of_week": {"S": days_of_week}}
    #     if start_time:
    #         request_item = {**request_item, "start_time": {"S": start_time}}
    #     if stop_time:
    #         request_item = {**request_item, "stop_time": {"S": stop_time}}
    #
    #     # TODO, cannot pass empty strings, or none. Make a request builder that does not pass those values
    #     request = {
    #         "TableName": self.__table_name,
    #         "Item": request_item
    #     }
    #
    #     # TODO, cannot pass empty strings, or none. Make a request builder that does not pass those values
    #     # Solved in the above portion. Keeping for posterity to ensure it behaves like I want
    #     # request = {
    #     #     "TableName": self.__table_name,
    #     #     "Item": {
    #     #         "pk": {
    #     #             "S": "period"
    #     #         },
    #     #         "sk": {
    #     #             "S": sort_key
    #     #         },
    #     #         "days_of_week": {
    #     #             "S": days_of_week
    #     #         },
    #     #         "start_time": {
    #     #             "S": start_time
    #     #         },
    #     #         "stop_time": {
    #     #             "S": stop_time
    #     #         }
    #     #     }
    #     # }
    #
    #     assert request_item == request.get('Item')
    #
    #     logger.info(
    #         f"Adding period item to {self.__table_name} with pk: period, sk: {sort_key} "
    #         f"start time: {start_time}, stop time: {stop_time}"
    #     )
    #
    #     response = self.dynamodb.put_item(**request)
    #     logger.info(f"HTTP Response: {response.get('ResponseMetadata').get('HTTPStatusCode')}")

    def retrieve_schedule_info(self, schedule_name: str):
        """
        :param schedule_name: str = schedule name as retrieved from AWS scheduler tag value
        :return:
        """
        logger.info(f"Attempting to retrieve periods from DynamoDB using pk: 'schedule', sk: '{schedule_name}'")

        # TODO: Check what happens when timezone isn't specified
        try:
            response = self.dynamodb.get_item(
                TableName=self.__table_name,
                Key={
                    "pk": {
                        'S': 'schedule'
                    },
                    "sk": {
                        'S': schedule_name
                    }
                },
                # timezone is a reserved key word
                ProjectionExpression="periods, #tz",
                ExpressionAttributeNames={
                    "#tz": "timezone"
                }
            )
        except botocore.exceptions.ClientError as err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=str(err),
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )
            # raise automated.exceptions.ClientError(err)
        except botocore.exceptions.EndpointConnectionError as err:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=str(err),
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )
            raise automated.exceptions.ConnectionError(err)

        period_list = None
        periods_with_timezone = []

        try:
            periods_with_timezone = data.convert_dynamo_json_to_py_data([response['Item']])
            # period_list = response.get('Item').get('periods').get('SS')
        except (AttributeError, KeyError) as e:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Unable to retrieve periods from schedule: [{schedule_name}] "
                              f"Response: {periods_with_timezone}. "
                              f"Are there any periods defined in the periods attribute for the schedule: "
                              f"[{schedule_name}] ?",
                include_in_http_response=True,
                fatal_error=False
            )

        return periods_with_timezone

    def retrieve_period_info(self, periods: list) -> list:
        """
        Takes a list of periods and retrieves the information associated with them from DynamoDB
        :param: periods: list = List of periods to retrieve information about
        :returns: list = period items and their associated information
        """
        requested_period_amount = len(periods)
        logger.info(f"Requesting period information for [{requested_period_amount}] periods: {periods}...")

        batch_list = []

        # Build the list of the periods we will batch get. This can be moved to a helper function
        for period in periods:
            key_structure = {
                "pk": {
                    "S": "period"
                },
                "sk": {
                    "S": period
                }
            }
            batch_list.append(key_structure)

        logger.debug(f"JSON in DynamoDB format being passed to batch_get_item: {batch_list}")

        response = self.dynamodb.batch_get_item(
            RequestItems={
                self.__table_name: {
                    'Keys': batch_list
                }
            }
        )
        logger.debug(f"Query response JSON: {response['Responses'][self.__table_name]}")
        response_period_amount = len(response['Responses'][self.__table_name])
        response_items = response['Responses'][self.__table_name]

        # We did not receive all requested items
        if requested_period_amount != response_period_amount:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Did not get response for all requested period items using {periods}. Evaluating... "
                              f"Requested: [{requested_period_amount}], Received: [{response_period_amount}].",
                include_in_http_response=True,
                fatal_error=False
            )
            # Find which period was not returned properly and log it
            for period in periods:
                if not any(response['sk']['S'] == period for response in response_items):
                    automated.exceptions.log_error(
                        automation_component=self,
                        error_message=f"Error in DynamoDB Batch Get Item when requested period information."
                                      f"Did not get proper response for all requested period items using: '{periods}' "
                                      f"Requested: '{requested_period_amount}', Received: '{response_period_amount}'. "
                                      f"Period '{period}' was not properly retrieved!",
                        include_in_http_response=True,
                        fatal_error=False
                    )

        if not len(response.get('Responses').get(self.__table_name)):
            logger.info(f"Unable to retrieve period information for '{periods}'")

        logger.debug(f"Received response with [{len(response['Responses'][self.__table_name])}] items.")
        logger.debug(f"Received all items? {len(periods) == len(response['Responses'][self.__table_name])}")

        converted_period_info = data.convert_dynamo_json_to_py_data(response['Responses'][self.__table_name])

        return converted_period_info

    # def _log_error(self, error_message: str, output_to_logger=True, include_in_http_response=True, fatal_error=False):
    #     if include_in_http_response:
    #         self.errors = error_message
    #     if output_to_logger:
    #         logger.error(error_message)
    #     if fatal_error:
    #         # TODO not implemented
    #         # How do I implement a return of this error and all previous errors from components?
    #         pass

    # # TESTING FUNCTION
    # def put_schedule_item(self, sort_key, period_set, timezone):
    #     """
    #     This might no longer be necessary except for testing, since we are using S3 or API GW to add/remove items
    #     to DynamoDB
    #     :return:
    #     """
    #
    #     breakpoint()
    #     request = {
    #         "TableName": self.__table_name,
    #         "Item": {
    #             "pk": {
    #                 "S": "schedule"
    #             },
    #             "sk": {
    #                 "S": sort_key
    #             },
    #             "periods": {
    #                 "SS": period_set
    #             },
    #             "timezone": {
    #                 "S": timezone
    #             }
    #         }
    #     }
    #
    #     try:
    #         response = self.dynamodb.put_item(**request)
    #     except botocore.exceptions.EndpointConnectionError as err:
    #         raise automated.exceptions.ConnectionError(err)
    #     except botocore.exceptions.ClientError as err:
    #         raise automated.exceptions.ClientError(err)
    #
    #     logger.info(
    #         f"Adding schedule item to {self.__table_name} with pk: schedule, sk: {sort_key} and timezone: {timezone}"
    #     )
