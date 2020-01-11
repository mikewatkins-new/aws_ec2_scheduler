import pytest
import automated.dynamodb
import config
import logging

# import docker

''' 
In order to run these tests we need to setup the local database connection
Use docker SDK for python to run container
 - docker run -p 8000:8000 amazon/dynamodb-local
 - Populate database with required tests
 - Execute tests
 - Terminate container
 OR just ensure these tests are run with the docker running amazon/dynamodb-local container running
 and use the appropriate endpoint:port to connect to it
'''

logger = logging.getLogger()

TABLE_NAME = "Scheduler"


# pytest fixture to be reused for all tests
@pytest.fixture(name="db_manager", scope="class", autouse=True)
def db_manager_fixture():
    class DBManager:
        def __init__(self):
            self.client = automated.dynamodb.DynamoDB(
                region="us-west-2",
                table_name="Scheduler",
                db_conn=config.DB_CONN_LOCAL
            )

    return DBManager()


class TestDynamoDB:
    """
    Assumption: Tests are executed in order.
    This is more functional testing than unit testing.
    Put the schedule and period information in the db.
    Retrieve the schedule and period information in the db and assert results are expected
    """

    # TODO: Test when batch write and get can't place all the items at once
    # TODO: Add complete clear of dynamo table so we can test only the values here

    # period_set_us = ["MON-THU", "FRI-START-10:00", "SUN-START-11:00-STOP-14:00"]
    # period_set_uk = ["TUE-SAT", "SUN-ALL-DAY", "THIRD_VALUE"]
    # dynamo_db.put_schedule_item("us_hours", period_set_us, "UTC")
    # dynamo_db.put_schedule_item("uk_hours", period_set_uk, "UTC")

    # sort_key, period_set, timezone
    schedule_information = [
        ("single_period_schedule", ["single_period"], "UTC"),
        ("multiple_period_schedule", ["single_period", "single_period_no_days"], "UTC"),
        ("us_hours", ["MON-THU", "FRI-START-10:00", "SUN-START-11:00-STOP-14:00"], "UTC"),
        ("uk_hours", ["TUE-SAT", "SUN-ALL-DAY", "THIRD_VALUE"], "UTC"),
        ("austin_offices", ["single_period"], "UTC"),
        ("new_york_offices", ["hyphenated_period_no_start", "single_period_no_days"], "UTC")
    ]

    # sort_key, days_of_week, start_time, stop_time
    period_information = [
        ("hyphenated_period", "MON-THU", "08:00", "18:00"),
        ("hyphenated_period_no_days", None, "08:00", "18:00"),
        ("hyphenated_period_no_start", "MON-THU", None, "18:00"),
        ("hyphenated_period_no_stop", "MON-THU", "08:00", None),
        ("single_period", "MON", "12:00", "24:00"),
        ("single_period_no_days", None, "12:00", "24:00"),
        ("single_period_no_start", "MON", None, "24:00"),
        ("single_period_no_stop", "MON", "12:00", None),
        ("MON-THU", "MON-THU", "08:00", "18:00"),
        ("FRI-START-10:00", "FRI", "10:00", "23:59"),
        ("SUN-START-11:00-STOP-14:00", "SUN", "10:00", "14:00"),
        ("TUE-SAT", "TUE-SAT", "00:30", "01:00"),
        ("SUN-ALL-DAY", "SUN", "08:00", "23:59"),
        ("SUN-NON-STOP", "SUN", "08:00", None)
    ]

    # @pytest.fixture(scope="class", autouse=True)
    # def setup_and_teardown(self):
    #     db = automated.dynamodb.DynamoDB(region="us-west-2", table_name="Scheduler", db_conn=config.DB_CONN_LOCAL)
    #     db._DynamoDB__create_table()
    #     yield
    #     pass

    # Can probably make these as test fixtures that run before the retrieval test
    # Since we are not asserting any responses or results
    @pytest.mark.parametrize(('schedule', 'period', 'timezone'), schedule_information)
    def test_put_schedule_item(self, schedule, period, timezone, db_manager):
        """
        Loads Schedule items into DynamoDB
        :param schedule: str = sort_key value of schedule item
        :param period: list = periods that are associated with the schedule
        :param timezone: str = timezone schedule is assoicated with
        :param db_manager: pytest fixture used to re-use DynamoDB object throughout tests
        :return: None
        """
        # response = db_manager.client.put_schedule_item(schedule, period, timezone, db_manager)
        self.put_schedule_item(schedule, period, timezone, db_manager)
        assert True
        # We don't get a return from this, so no response?
        # Should we add a response?

    @pytest.mark.parametrize(('sort_key', 'days_of_week', 'start_time', 'stop_time'), period_information)
    def test_put_period_item(self, sort_key, days_of_week, start_time, stop_time, db_manager):
        # response = db_manager.client.put_period_item(sort_key, days_of_week, start_time, stop_time)
        self.put_period_item(sort_key, days_of_week, start_time, stop_time, db_manager)
        # We don't get a return from this, so no response?
        # Should we add a response?

    @pytest.mark.parametrize(('sort_key', 'period_list', 'timezone'), schedule_information)
    def test_retrieve_periods(self, sort_key, period_list, timezone, db_manager):
        response = db_manager.client.retrieve_schedule_info(sort_key)
        # DynamoDB sets do not preserve order. Either sort both lists before assertsions or use a dynamo list for order
        periods = None
        for val in response:
            periods = val.get('periods')
            timezone = val.get('timezone')
            period_info = list(periods)

        period_info.sort()
        period_list.sort()
        assert period_info == period_list
        # [print(x) for x in response]

    @pytest.mark.parametrize(('sort_key', 'days_of_week', 'start_time', 'stop_time'), period_information)
    def test_retrieve_period_info(self, sort_key, days_of_week, start_time, stop_time, db_manager):
        # db_manager.client.put_period_item(sort_key, days_of_week, start_time, stop_time)
        response = db_manager.client.retrieve_period_info([sort_key])
        assert response[0].get('sk') == sort_key
        assert response[0].get('days_of_week') == days_of_week
        assert response[0].get('start_time') == start_time
        assert response[0].get('stop_time') == stop_time

    def put_schedule_item(self, sort_key, period_set, timezone, db_manager):
        """
        Helper function to put schedule items individually rather than using a whole config file
        Later I will add the capability to generate an entire config that can be used to test against
        """

        request = {
            "TableName": TABLE_NAME,
            "Item": {
                "pk": {
                    "S": "schedule"
                },
                "sk": {
                    "S": sort_key
                },
                "periods": {
                    "SS": period_set
                },
                "timezone": {
                    "S": timezone
                }
            }
        }

        response = db_manager.client.dynamodb.put_item(**request)

        # try:
        #     response = self.dynamodb.put_item(**request)
        # except botocore.exceptions.EndpointConnectionError as err:
        #     raise automated.exceptions.ConnectionError(err)
        # except botocore.exceptions.ClientError as err:
        #     raise automated.exceptions.ClientError(err)

    # TODO: Is this only for testing now? Can probably move this to testing.
    def put_period_item(self, sort_key, days_of_week, start_time, stop_time, db_manager):
        """
        :return:
        """

        # Attributes we require for putting an item
        request_item = {
            "pk": {
                "S": "period"
            },
            "sk": {
                "S": sort_key
            }
        }

        # Is this bad practice? Does it slow things down unnecessarily?
        # Perhaps I can start with all attributes and then .pop remove the ones I don't want for speed
        if days_of_week:
            request_item = {**request_item, "days_of_week": {"S": days_of_week}}
        if start_time:
            request_item = {**request_item, "start_time": {"S": start_time}}
        if stop_time:
            request_item = {**request_item, "stop_time": {"S": stop_time}}

        # TODO, cannot pass empty strings, or none. Make a request builder that does not pass those values
        request = {
            "TableName": TABLE_NAME,
            "Item": request_item
        }

        # TODO, cannot pass empty strings, or none. Make a request builder that does not pass those values
        # Solved in the above portion. Keeping for posterity to ensure it behaves like I want
        # request = {
        #     "TableName": self.__table_name,
        #     "Item": {
        #         "pk": {
        #             "S": "period"
        #         },
        #         "sk": {
        #             "S": sort_key
        #         },
        #         "days_of_week": {
        #             "S": days_of_week
        #         },
        #         "start_time": {
        #             "S": start_time
        #         },
        #         "stop_time": {
        #             "S": stop_time
        #         }
        #     }
        # }

        assert request_item == request.get('Item')

        logger.info(
            f"Adding period item to {TABLE_NAME} with pk: period, sk: {sort_key} "
            f"start time: {start_time}, stop time: {stop_time}"
        )

        response = db_manager.client.dynamodb.put_item(**request)
        logger.info(f"HTTP Response: {response.get('ResponseMetadata').get('HTTPStatusCode')}")

# import pytest
#
# def pytest_namespace():
#     return {'my_global_variable': 0}
#
# @pytest.fixture
# def data():
#     pytest.my_global_variable = 100
#
# def test(data):
#     print(pytest.my_global_variable)
