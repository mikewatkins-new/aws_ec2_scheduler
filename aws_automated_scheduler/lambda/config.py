import logging
import events.type

LOGGING_LEVEL = logging.INFO
VERBOSE_LOGGING = True

# TESTING_EVENT = events.type.API_RETRIEVE_DYNAMO_AS_CONFIG
# TESTING_EVENT = events.type.API_S3_PUT_CONFIG
TESTING_EVENT = events.type.CW_SCHEDULED_EVENT

USE_PRETTY_JSON = True

RECOVERABLE_ERROR = 1

# USE_MOCK_EC2 = False

S3_CONN_LOCAL = "s3_conn_local"
S3_CONN_DEFAULT = "s3_conn_default"

EC2_CONN_LOCAL = "ec2_conn_local"
EC2_CONN_DEFAULT = "ec2_conn_default"

# Database connection options
DB_CONN_LOCAL = "db_conn_local"
DB_CONN_LOCAL_ENDPOINT = "http://127.0.0.1:8000"
# DB_CONN_LOCAL_ENDPOINT = "http://192.168.99.100:8000"
DB_CONN_SERVERLESS = "db_conn_serverless"


def is_test_run() -> bool:
    testing = False
    try:
        if TESTING_EVENT:
            testing = True
    except NameError:
        pass

    return testing
