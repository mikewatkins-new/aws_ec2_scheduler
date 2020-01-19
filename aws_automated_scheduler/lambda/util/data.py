import decimal
import logging
import json
import config
import automated.exceptions
import events.http_response as http_response

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

logger = logging.getLogger()


def validate_json(s3_object_data: str) -> list:
    """
    Validate data to ensure it is proper JSON format by de-serializing it and catching exceptions
    """
    logger.info("Validating JSON...")
    valid_json = None

    try:
        valid_json = json.loads(s3_object_data)
        logger.info("JSON validated.")

    except json.decoder.JSONDecodeError as err:
        automated.exceptions.log_error(
            automation_component=None,
            error_message=f"Encountered invalid JSON when attempting to decode {s3_object_data} {err}. ",
            output_to_logger=True,
            include_in_http_response=False,
            fatal_error=True
        )

    return valid_json


def convert_json_to_dynamo_json(input_json: list) -> list:
    """
    Re-Serializes the data into DynamoDB compatible JSON that can be used to put items into the dynamo table
    """
    # HACK: When serializing the JSON without dynamodb attribute types included, it wants to convert
    # The DynamoDB 'String Set' objects to DynamoDB List objects because python loads the data as lists and not sets.
    # I am choosing to go with DynamoDB attribute string sets, because I do not want duplicate entries for periods,
    # and it is easier to parse visually. The only drawback I have seen so far is that sets are unordered,
    # but since we are not evaluating the period string set responses in any particular order that should not matter.
    # Can we change this to use cls instead of for loops?

    serializer = TypeSerializer()
    py_data, json_data = [], []

    logger.info(f"Converting JSON config to DynamoDB compatible JSON.")

    # Loop through JSON file data looking for Python object type list
    # Convert the list object into a set of strings
    # Store new data types as python object
    for data in input_json:
        for k, v in data.items():
            if isinstance(v, list):
                data[k] = set(v)
        py_data.append(data)

    # Serialize previously modified python object data into DynamoDB JSON
    for data in py_data:
        dynamo_data = {k: serializer.serialize(v) for k, v in data.items()}
        json_data.append(dynamo_data)

    return json_data


def convert_dynamo_json_to_py_data(input_json: list) -> list:
    """

    :param input_json: DynamoDB compatible JSON with included attributes
    :return: py_data: DynamoDB incompatible JSON with removed attributes
    """
    py_data = []
    deserializer = TypeDeserializer()
    for item in input_json:
        json_data = {k: deserializer.deserialize(v) for k, v in item.items()}
        py_data.append(json_data)

    logger.debug(f"Stripped attributes result: {py_data}")
    return py_data


def write_json_to_file(input_json, file_name="automated_config.json", use_pretty_json=config.USE_PRETTY_JSON) -> None:
    logger.info(f"Writing JSON to [{file_name}]")
    with open(file_name, 'w') as f:
        if use_pretty_json:
            f.write(human_readable_json(input_json))
        else:
            f.write(machine_readable_json(input_json))


def human_readable_json(input_json) -> str:
    return json.dumps(input_json, indent=4, sort_keys=True, cls=DynamoEncoder)


def machine_readable_json(input_json) -> str:
    return json.dumps(input_json, cls=DynamoEncoder)


# Helper class to convert DynamoDB object to JSON taken from AWS example code
class DynamoEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            if o % 1 > 0:
                return float(o)
            else:
                return int(o)
        elif isinstance(o, set):
            return list(o)
        return super(DynamoEncoder, self).default(o)
