import logging
import pytest
import util.evalperiod
import automated.ec2_actions
from datetime import datetime

logger = logging.getLogger()


class TestEvalPeriod:
    # TODO: Implement test for start/stop time These should include:
    # No days
    # No start time
    # No stop time
    # Not yet at start time
    # Between times
    # After stop time
    # 24:00
    # 00:00
    # Check on a new day, missing previous stop action

    # Object is passed with datetime representing date/time to test our evaluation against
    # We only care about the day of the calendar and hour, so the year only matters for establishing what the day is
    # e.g. MON, TUE, WED, ...
    testing_datetime = {
        "MON-06:00": datetime(year=2020, month=1, day=6, hour=6, minute=00, second=0),
        "MON-12:00": datetime(year=2020, month=1, day=6, hour=12, minute=00, second=0),
        "MON-18:00": datetime(year=2020, month=1, day=6, hour=18, minute=00, second=0),
        "MON-22:30": datetime(year=2020, month=1, day=6, hour=22, minute=30, second=0),
        "TUE-06:00": datetime(year=2020, month=1, day=7, hour=6, minute=00, second=0),
        "TUE-12:00": datetime(year=2020, month=1, day=7, hour=12, minute=00, second=0),
        "TUE-18:00": datetime(year=2020, month=1, day=7, hour=18, minute=00, second=0),
        "TUE-22:30": datetime(year=2020, month=1, day=7, hour=22, minute=30, second=0),
        "WED-06:00": datetime(year=2020, month=1, day=8, hour=6, minute=00, second=0),
        "WED-12:00": datetime(year=2020, month=1, day=8, hour=12, minute=00, second=0),
        "WED-18:00": datetime(year=2020, month=1, day=8, hour=18, minute=00, second=0),
        "WED-22:30": datetime(year=2020, month=1, day=8, hour=22, minute=30, second=0),
        "THU-06:00": datetime(year=2020, month=1, day=9, hour=6, minute=00, second=0),
        "THU-12:00": datetime(year=2020, month=1, day=9, hour=12, minute=00, second=0),
        "THU-18:00": datetime(year=2020, month=1, day=9, hour=18, minute=00, second=0),
        "THU-22:30": datetime(year=2020, month=1, day=9, hour=22, minute=30, second=0),
        "FRI-06:00": datetime(year=2020, month=1, day=10, hour=6, minute=00, second=0),
        "FRI-12:00": datetime(year=2020, month=1, day=10, hour=12, minute=00, second=0),
        "FRI-18:00": datetime(year=2020, month=1, day=10, hour=18, minute=00, second=0),
        "FRI-22:30": datetime(year=2020, month=1, day=10, hour=22, minute=30, second=0),
        "SAT-06:00": datetime(year=2020, month=1, day=11, hour=6, minute=00, second=0),
        "SAT-12:00": datetime(year=2020, month=1, day=11, hour=12, minute=00, second=0),
        "SAT-18:00": datetime(year=2020, month=1, day=11, hour=18, minute=00, second=0),
        "SAT-22:30": datetime(year=2020, month=1, day=11, hour=22, minute=30, second=0),
        "SUN-06:00": datetime(year=2020, month=1, day=12, hour=6, minute=00, second=0),
        "SUN-12:00": datetime(year=2020, month=1, day=12, hour=12, minute=00, second=0),
        "SUN-18:00": datetime(year=2020, month=1, day=12, hour=18, minute=00, second=0),
        "SUN-22:30": datetime(year=2020, month=1, day=12, hour=22, minute=30, second=0),
    }

    # Simulates the returned object from DynamoDB representing a period, its days, and start/stop time
    period_information = [
        ({"sk": "hyphenated_period", "days_of_week": "MON-THU", "start_time": "08:00", "stop_time": "18:00"},
         testing_datetime['MON-06:00'], automated.ec2_actions.NONE),
        ({"sk": "hyphenated_period", "days_of_week": "MON-THU", "start_time": "08:00", "stop_time": "18:00"},
         testing_datetime['MON-12:00'], automated.ec2_actions.START),
        ({"sk": "hyphenated_period", "days_of_week": "MON-THU", "start_time": "08:00", "stop_time": "18:00"},
         testing_datetime['MON-18:00'], automated.ec2_actions.STOP),
        ({"sk": "hyphenated_period", "days_of_week": "MON-THU", "start_time": "08:00", "stop_time": "18:00"},
         testing_datetime['MON-22:30'], automated.ec2_actions.STOP),
        # {"sk": "hyphenated_period_no_days", "days_of_week": None, "start_time": "08:00", "stop_time": "18:00"},
        # {"sk": "hyphenated_period_no_start", "days_of_week": "MON-THU", "start_time": None, "stop_time": "18:00"},
        # {"sk": "hyphenated_period_no_stop", "days_of_week": "MON-THU", "start_time": "08:00", "stop_time": None},
        # {"sk": "single_period", "days_of_week": "MON", "start_time": "08:00", "stop_time": "18:00"},
        # {"sk": "single_period_no_days", "days_of_week": None, "start_time": "08:00", "stop_time": "18:00"},
        # {"sk": "single_period_no_start", "days_of_week": "MON", "start_time": None, "stop_time": "18:00"},
        # {"sk": "single_period_no_stop", "days_of_week": "MON", "start_time": "08:00", "stop_time": None}
    ]

    # Used for testing evalperiod.__is_matching_day
    matching_day = [
        (datetime(year=2020, month=1, day=6), "MON", True),
        (datetime(year=2020, month=1, day=7), "TUE", True),
        (datetime(year=2020, month=1, day=8), "WED", True),
        (datetime(year=2020, month=1, day=9), "THU", True),
        (datetime(year=2020, month=1, day=10), "FRI", True),
        (datetime(year=2020, month=1, day=11), "SAT", True),
        (datetime(year=2020, month=1, day=12), "SUN", True),
        (datetime(year=2020, month=1, day=6), "mon", True),
        (datetime(year=2020, month=1, day=7), "tue", True),
        (datetime(year=2020, month=1, day=8), "wed", True),
        (datetime(year=2020, month=1, day=9), "thu", True),
        (datetime(year=2020, month=1, day=10), "fri", True),
        (datetime(year=2020, month=1, day=11), "sat", True),
        (datetime(year=2020, month=1, day=12), "sun", True),
        (datetime(year=2020, month=1, day=6), "value_error", False),
        (datetime(year=2020, month=1, day=6), "", False),
        (datetime(year=2020, month=1, day=6), None, False),
        (datetime(year=2020, month=1, day=6), "WED", False),
        (datetime(year=2020, month=1, day=2), "wed", False),
        (datetime(year=2020, month=1, day=6), "mon-wed", True),
        (datetime(year=2020, month=1, day=6), "MON-WED", True),
        (datetime(year=2020, month=1, day=6), "MON-WED", True),
        (datetime(year=2020, month=1, day=6), "MON,WED", True),
        (datetime(year=2020, month=1, day=6), "MON-TUE,WED", True),
        (datetime(year=2020, month=1, day=9), "MON-WED,FRI-SUN", False),
    ]

    @pytest.mark.parametrize(('period', 'testing_days', 'expected_result'), period_information)
    def test_eval_period(self, period, testing_days, expected_result):
        evaluator = util.evalperiod.EvalPeriod()
        action = evaluator.eval_period(period=period, override_time=testing_days)
        assert action == expected_result
        # assert False
        # Right now this test only tests that nothing breaks when calling the function.
        # There is no assertion since the result will change based on the time of day.
        # I will probably modify it so we can pass a specific datetime object so we can evaluate what it should be.

    @pytest.mark.parametrize(('date_time', 'days_of_week', 'expected_result'), matching_day)
    def test_is_matching_day(self, date_time, days_of_week, expected_result):
        evaluator = util.evalperiod.EvalPeriod()
        dt = evaluator._EvalPeriod__is_matching_day(date_time, days_of_week)
        assert dt == expected_result
