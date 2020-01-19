import automated.dynamodb
import automated.s3
import logging
import automated.exceptions
import config
import events.http_response as http_response
from util import data

logger = logging.getLogger()


def retrieve_dynamo_as_config(env_vars: dict):
    region = env_vars.get("region")
    table_name = env_vars.get("table_name")

    # If TESTING, use local database connection, otherwise use default (config.DB_CONN_SERVERLESS)
    test_run = config.is_test_run()
    if test_run:
        dynamodb = automated.dynamodb.DynamoDB(region=region, table_name=table_name, db_conn=config.DB_CONN_LOCAL)
    else:
        dynamodb = automated.dynamodb.DynamoDB(region=region, table_name=table_name, db_conn=config.DB_CONN_SERVERLESS)

    response = dynamodb.retrieve_all_items_from_dynamo()
    converted_json = data.convert_dynamo_json_to_py_data(response)

    # TODO: Currently this outputs it as a JSON compatible HTTP response.
    #  Consider outputting to S3 Object (watch out for infinite loop on S3 PutObject).

    if test_run:
        # Output to file so we can test locally and see the response JSON
        data.write_json_to_file(converted_json, use_pretty_json=True)

    # TODO: Add error checking before sending good response

    return http_response.construct_http_response(
        status_code=http_response.OK,
        message=str(converted_json)
    )
