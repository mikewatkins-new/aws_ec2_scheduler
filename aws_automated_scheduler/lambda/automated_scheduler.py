import util.data
import util.evalperiod
import util.eventhandler
import logging
import config

# Change default Lambda logging behavior/format to custom format
logger = logging.getLogger()

root = logging.getLogger()
if root.handlers:
    for handler in root.handlers:
        root.removeHandler(handler)

logger.setLevel(config.LOGGING_LEVEL)
logging.basicConfig(format='%(asctime)s [%(levelname)s] %(message)s', level=config.LOGGING_LEVEL)


def event_handler(event, context) -> dict:
    """
    Main starting point of application. Handles the incoming events from caller (CloudWatch, API GW):
        - API call via CloudTrail event from S3 PutObject to put new config (schedules and periods)
        - API call via API Gateway to dump current DynamoDB table as config for reading/writing
        - CloudWatch Event scheduled task for triggering main scheduler application
    :param event: Caller event the lambda function receives
    :param context:
    :return: dict = HTTP response returned to caller
    """

    '''
    TODO: Consider making all ec2 start/stop calls a single batch API call rather than individual start/stop
        The downside to this is fatal errors and timeouts will have to be considered more or all actions will fail
    TODO: send JSON payload directly via API GW for placing config to DynamoDB instead of uploading config to S3
    CONSIDER: API call for check mode to output what actions would happen to what resource over day or time. 
            Would give feedback to check if scheduling behavior will work as intended.
    '''

    automated_event_handler = util.eventhandler.AutomationEventHandler(event, context)
    http_response: dict = automated_event_handler.evaluate_event()
    log_http_response(http_response=http_response)

    return http_response


def log_http_response(http_response: dict) -> None:
    if config.USE_PRETTY_JSON:
        logger.info(f"Response:\n{util.data.human_readable_json(http_response)}")
    else:
        logger.info(f"Response:\n{util.data.machine_readable_json(http_response)}")


# TESTING: For local testing. Launch point.
if __name__ == '__main__':
    if config.is_test_run():
        event_handler({'detail-type': config.TESTING_EVENT}, {})
    else:
        event_handler({'detail-type': 'Scheduled Event'}, {})
        # event_handler({'detail-type': 'AWS API Call via CloudTrail', 'detail': {'eventName': 'PutObject'}}, {})
