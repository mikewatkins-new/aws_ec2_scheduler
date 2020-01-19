import automated.dynamodb
import automated.s3
import logging
import events.type
import automated.exceptions
import config
import sys
from util import data
import events.http_response as http_response

logger = logging.getLogger()


def put_config_into_dynamo(env_vars) -> dict:
    """
    Puts a JSON config of schedule and period items into DynamoDB
    :param env_vars: Environment variables retrieved from Lambda
    :return:
    """

    # received_event_bucket = event['requestParameters']['bucketName']
    # received_event_key = event['requestParameters']['Key']

    region: str = env_vars.get("region")
    table_name: str = env_vars.get("table_name")

    test_run: bool = config.is_test_run()

    if test_run:
        s3 = automated.s3.S3(s3_conn=config.S3_CONN_LOCAL)
        dynamodb = automated.dynamodb.DynamoDB(region=region, table_name=table_name, db_conn=config.DB_CONN_LOCAL)
    else:
        s3 = automated.s3.S3(s3_conn=config.S3_CONN_DEFAULT)
        dynamodb = automated.dynamodb.DynamoDB(region=region, table_name=table_name, db_conn=config.DB_CONN_SERVERLESS)

    s3_object_data: str = s3.retrieve_data_from_s3_object()
    validated_json: list = data.validate_json(s3_object_data)
    converted_json: list = data.convert_json_to_dynamo_json(validated_json)

    logger.info("Config JSON to DynamoDB compatible JSON conversion successful")

    response: dict = dynamodb.load_json_into_db(converted_json)
    response_status_code: str = response.get('ResponseMetadata').get('HTTPStatusCode')

    # This returns a set or dict, not a list? We can change this to be something other than length, depending on..
    # TODO: Implement logic to handle batch writes that need to pass unprocessed items
    if len(response.get('UnprocessedItems')):
        logger.info("FOUND UNPROCESSED ITEMS... FIGURE OUT WHAT TO DO NEXT!")
        logger.info(f"Unprocessed Items: {response.get('UnprocessedItems')}")

    elif len(response.get('UnprocessedItems')) == 0 and response_status_code == http_response.OK:
        logger.info("Successfully loaded items into DynamoDB")
        return http_response.construct_http_response(
            status_code=http_response.OK,
            message=f"Success from '{events.type.API_S3_PUT_CONFIG}'"
        )

    else:
        return http_response.construct_http_response(
            status_code=response_status_code,
            message=str(response)
        )
