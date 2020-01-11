from boto3 import resource
import logging
import config
import os
import automated.exceptions


logger = logging.getLogger()


class S3:
    """

    """

    def __init__(self, s3_conn=config.S3_CONN_DEFAULT):
        """

        """
        self.__s3_conn = s3_conn
        self.__testing = config.is_test_run()
        self._s3 = resource('s3')
        self.__errors = []

        if self.__testing:
            logger.warning(f"[TESTING] Using mock S3 connection")

        try:
            self.__bucket = os.environ['scheduler_bucket_name']
            logger.debug(f"Discovered S3 bucket name <{self.__bucket}> from environment variable 'scheduler_bucket_name'")
        except KeyError:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Error: No bucket name found. Please set 'scheduler_bucket_name' environment variable.",
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )

        try:
            self.__s3_config_object_key = os.environ['scheduler_s3_config_object_key']
            logger.debug(f"Discovered S3 object key name [{self.__s3_config_object_key}] from environment variable "
                         f"'scheduler_s3_config_object_key")
        except KeyError:
            automated.exceptions.log_error(
                automation_component=self,
                error_message=f"Error: No object key found. Please set 'scheduler_s3_config_object_key' env variable.",
                output_to_logger=True,
                include_in_http_response=True,
                fatal_error=True
            )

    @property
    def errors(self):
        return self.__errors

    @errors.getter
    def errors(self):
        return self.__errors

    @errors.setter
    def errors(self, error_message):
        self.__errors.append(error_message)

    def retrieve_data_from_s3_object(self) -> str:
        """
        Gets object data from s3 and stores it as python data object or json
        """

        file_content = None
        bucket = self.__bucket
        object_key = self.__s3_config_object_key

        logger.info(f"Retrieving config file from s3:{bucket}/{object_key}")

        # Check if we are doing local testing. If local testing retrieve file from directory
        if self.__s3_conn == config.S3_CONN_LOCAL:
            object_key = 'automated_config.json'
            with open(object_key, 'r') as f:
                file_content = f.read()

        # TODO: See which one works better.
        else:

            obj = self._s3.Object(bucket, object_key)
            file_content = obj.get()['Body'].read()
            print(file_content)

            response = self._s3.get_object(Bucket=bucket, Key=object_key)
            file_content = response['Body'].read().decode('utf-8')
            print(file_content)

        return file_content

    def retrieve_formatted_data(self):
        """
        returns data so it can be passed to dynamo to create these things:
        (X) Schema: The schema is loaded from the CDK application deploy
        (X) Table: The table is created from the CDK application deploy
        (/) Attributes: This will be what is changed on s3 put object
        TODO: Check to see what happens when attribute exists
        """
        pass
