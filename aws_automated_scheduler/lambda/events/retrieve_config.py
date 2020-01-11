import automated.dynamodb
import automated.s3
import logging
import automated.exceptions
import config
import events.http_response as http_response
from util import data

logger = logging.getLogger()


def retrieve_dynamo_as_config(env_vars):
    region = env_vars.get("region")
    table_name = env_vars.get("table_name")

    # If TESTING, use local database connection, otherwise use default (config.DB_CONN_SERVERLESS)
    testing = config.is_test_run()
    if testing:
        dynamodb = automated.dynamodb.DynamoDB(region=region, table_name=table_name, db_conn=config.DB_CONN_LOCAL)
    else:
        dynamodb = automated.dynamodb.DynamoDB(region=region, table_name=table_name)

    # Retrieves all DynamoDB items including as JSON, including attributes
    response = dynamodb.retrieve_all_items_from_dynamo()

    # Remove all attributes from JSON so we can use it as more readable config
    converted_json = data.convert_dynamo_json_to_py_data(response)

    # TODO: Currently this outputs it as a JSON compatible HTTP response.
    #  Consider outputting to S3 Object (watch out for infinite loop on S3 PutObject).

    if testing:
        # Output to file so we can test locally and see the response JSON
        data.write_json_to_file(converted_json, use_pretty_json=True)

    # TODO: Add error checking before sending good response

    return http_response.construct_http_response(
        status_code=http_response.OK,
        message=converted_json
    )
