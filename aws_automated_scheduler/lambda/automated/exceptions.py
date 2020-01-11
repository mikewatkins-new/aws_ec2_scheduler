import json
import sys
import logging
import events.http_response as http_response

RECOVERABLE_ERROR = 1

logger = logging.getLogger()


def log_error(automation_component,
              error_message: str,
              output_to_logger: bool = True,
              include_in_http_response: bool = True,
              http_status_code: int = 500,
              fatal_error: bool = False):

    if output_to_logger:
        logger.error(error_message)

    # Add error to class object. Useful for non-fatal errors that can be retrieved from components later
    if include_in_http_response:
        automation_component.errors = error_message

    if fatal_error:
        # if http_status_code is None:
        #     http_status_code = 500
        # TODO How do I implement a return of this error and all previous errors from other components?
        # This will ignore minor errors from previous components and return only the fatal error
        raise FatalError(http_status_code=http_status_code, expression=error_message)


class Error(Exception):
    """
    Base class for exception
    """
    pass


class NoRegionSpecified(Error):
    """

    """

    def __init__(self):
        logger.error(f"Error: No region found. Please set 'scheduler_region' environment variable.")
        sys.exit(RECOVERABLE_ERROR)


class NoTagSpecified(Error):
    """

    """

    def __init__(self):
        logger.error(f"Error: No tag key found. Please set 'scheduler_tag' environment variable.")
        sys.exit(RECOVERABLE_ERROR)


class NoTableSpecified(Error):
    """

    """

    def __init__(self):
        logger.error(f"Error: No table name found. Please set 'scheduler_table' environment variable.")
        sys.exit(RECOVERABLE_ERROR)


class NoBucketNameSpecified(Error):
    """

    """

    def __init__(self):
        logger.error(f"Error: No bucket name found. Please set 'scheduler_bucket_name' environment variable.")
        sys.exit(RECOVERABLE_ERROR)


class NoConfigObjectKeySpecified(Error):
    """

    """

    def __init__(self):
        logger.error(
            f"Error: No config object key found. "
            f"Please set 'scheduler_s3_config_object_key' environment variable."
        )
        sys.exit(RECOVERABLE_ERROR)


class NoEC2InstancesFound(Error):
    """

    """

    def __init__(self, expression):
        logger.warning(f"No EC2 instances found that are tagged with automation scheduling tag: {expression}.")
        pass


class FatalError(Error):
    """
    Example: An error occurred (AccessDeniedException) when calling the GetItem operation:
    # User: arn:aws:iam::{account_id}:user/automated_local is not authorized to perform: dynamodb...
    """

    def __init__(self, http_status_code, expression):
        self.__http_status_code = http_status_code
        self.__expression = expression

        # Just log the error, being called by log_error, avoid infinite loop by not recalling fatal_error
        # Only using for logging purposes.
        logger.error(f"Fatal error received. Terminating application and returning HTTP response.")
        # log_error(
        #     automation_component=self,
        #     error_message=f"Fatal error received. Returning HTTP response.",
        #     include_in_http_response=False,
        #     output_to_logger=True,
        #     fatal_error=False  # Prevent infinite loop
        # )

        response = http_response.construct_http_response(
            status_code=http_status_code,
            message=expression
        )
        sys.exit(
            json.dumps(response, indent=2, sort_keys=True)
        )


class ClientError(Error):
    """
    Example: An error occurred (AccessDeniedException) when calling the GetItem operation:
    # User: arn:aws:iam::{account_id}:user/automated_local is not authorized to perform: dynamodb...
    """

    def __init__(self, expression):
        self.expression = expression

        logger.error(expression)
        sys.exit(RECOVERABLE_ERROR)


class ConnectionError(Error):

    def __init__(self, expression):
        self.expression = expression
        logger.error(expression)
        sys.exit(RECOVERABLE_ERROR)
