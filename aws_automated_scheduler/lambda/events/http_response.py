NOT_FOUND = 404
OK = 200
INTERNAL_ERROR = 500

def construct_http_response(status_code, message) -> dict:
    # Handle any errors or response manipulations here.

    # Message is being passed as a string, no reason to serialize it?
    # message = json.loads(message)
    # message = util.data.get_machine_readable_json(message)

    body: dict = {
        "message": message
    }

    return {
        "statusCode": status_code,
        "body": body
    }
