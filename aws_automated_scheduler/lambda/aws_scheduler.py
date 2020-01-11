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
    Main starting portion of application. Handles the incoming events from caller (CloudWatch, API GW):
        - API call via CloudTrail event from S3 PutObject to put new config (schedules and periods)
        - API call via API Gateway to dump current DynamoDB table as config for reading/writing
        - CW Event scheduled task for trigger main scheduler application
    :param event: Event the lambda function receives
    :param context:
    :return: dict = HTTP response returned to caller.
    """

    '''
    TODO: Consider making all ec2 start/stop calls a single batch API call rather than individual start/stop
    TODO: send JSON directly via API GW for inputting config to DynamoDB
    CONSIDER: API call for check mode to output what actions would happen to what resource over day or time. 
            Would give feedback to check if scheduling behavior will work as intended.
    '''

    # TESTING: Used to simplify local testing. Simulates event specified in config.
    if config.is_test_run():
        event = config.TESTING_EVENT

    # Pass the event to the automated event handler to determine appropriate response
    automated_event_handler = util.eventhandler.AutomationEventHandler(event, context)

    http_response = automated_event_handler.evaluate_event()

    # Output response information to log. If Lambda logs the return this information will be redundant.
    # TODO: Check if Lambda is logging the response object as well.
    if config.USE_PRETTY_JSON:
        logger.info(f"Response from Lambda:\n{util.data.get_human_readable_json(http_response)}")
    else:
        logger.info(f"Response from Lambda:\n{util.data.get_machine_readable_json(http_response)}")

    # Exit response to caller service (CloudWatch event, API GW)
    return http_response


# TESTING: For local testing launch point

if __name__ == '__main__':
    event_handler({'detail-type': 'Scheduled Event'}, {})
    # event_handler({'detail-type' : 'AWS API Call via CloudTrail', 'detail': {'eventName': 'PutObject'}}, {})
