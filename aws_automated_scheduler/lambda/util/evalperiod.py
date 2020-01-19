from datetime import datetime
import time
import logging
import automated.ec2_actions as ec2_actions

# import pytz

logger = logging.getLogger()


class EvalPeriod:
    """

    """

    def __init__(self):
        self._errors = []

    @property
    def errors(self):
        return self._errors

    @errors.setter
    def errors(self, message):
        self._errors.append(message)

    @errors.getter
    def errors(self):
        return self._errors

    def eval_period(self, period, override_time: datetime = None) -> str:

        """
        Might just consider passing the whole period structure to this function
        Evaluate the period and parse the days and start/stop times to see if we are in the window
        and what the action should be
        :param: override_time: datetime: Used for testing to set an exact time to test against
        :return:
        """
        matching_day: bool = False
        action_type: str = ec2_actions.NONE

        logger.info(f"Evaluating period: [{period}]")

        # Allow none type to be returned
        # No days means it never gets used
        # No start time means this period rule does not start it automatically
        # No stop time means this period rule does not stop it automatically
        # Removed .lower because it was causing the Nonetype to fail
        days_of_week = period.get("days_of_week")
        start_time = period.get("start_time")
        stop_time = period.get("stop_time")

        # TODO Add timezone implementations, start with UTC only for simplicity
        current_date_time = None
        if override_time:
            current_date_time = override_time
        else:
            current_date_time = self.__current_date_time("UTC")

        logger.info(f"Current datetime: [{current_date_time}]")
        logger.info(f"Current abbreviated day of week: [{current_date_time.strftime('%a').lower()}]")
        logger.info(f"Period info: days of week: [{days_of_week}] start time [{start_time}] stop time: [{stop_time}]")

        # Allow dates to be single specific day or hyphenated for range
        # TODO later implement ability to comma seperated days or combination of comma and hyphen
        # Ex: mon,tues,thu and mon-tue,thur

        # Check to see if our current day matches the days of week for the period
        matching_day = self.__is_matching_day(current_date_time, days_of_week)
        logger.info(f"Matching day: [{matching_day}]")

        # Evaluate date times compared to now
        # If current_date_time is within our period date_time then flag as action needed
        # day = current_date_time.day
        # hour = current_date_time.hour
        # minute = current_date_time.minute

        # Return action needed

        if matching_day:
            logger.info(f"Found matching day in days_of_week attribute, evaluating schedule time...")
            action_type = self.__actionable_time(current_date_time, start_time, stop_time)
            if action_type is not ec2_actions.NONE:
                logger.info(f"Found matching time, returning action type: <{action_type}>")

        return action_type

    def __current_date_time(self, timezone):
        """
        Gets the current time in the supplied timezone
        :return:
        """
        # Can try using pytz and timezones
        # or just datetime.datetime.utcnow()
        # current_date_time = datetime.astimezone(tz=pytz.utc)
        return datetime.utcnow()

    def __is_matching_day(self, current_date_time: datetime, days_of_week: str) -> bool:

        # TODO: Remove early return, and decide if no defined days means no day or all days
        # I am leaning towards no action for that, but why would you ever not define a day?
        # That makes no sense, maybe day should be required.
        if days_of_week is None or days_of_week == "":
            logger.warning(f"There are no days listed for the period. Not evaluating further days...")
            return False

        matching = False

        # import calendar
        # mon = 0, tue = 1, wed = 2, thu = 3, fri = 4, sat = 5, sun = 6
        # day_mapping = dict(zip(range(7), calendar.day_abbr))
        # day_str_mapping = dict(zip(calendar.day_abbr, range(7)))
        # Try converting all the days to lower, they are calendar object so .lower() doesn't work
        # Have to create a new dictionary using those values

        logger.info(f"days_of_week before split: {days_of_week}")
        days_as_list = days_of_week.split(',')
        logger.info(f"days_of_week after split: {days_as_list}")
        logger.info(f"Current weekday as integer: [{current_date_time.weekday()}]")

        for day_set in days_as_list:

            # Check if we have a single day listed or set of days denoted by a hyphen, e.g. [MON-WED]
            if '-' in day_set:
                split_days = day_set.split('-')
                assert (len(split_days) == 2)

                # TODO: What happens if we go over, like SAT-TUE??
                # TODO We need to be able safely continue on error, but also collect all errors for information
                # TODO at the end of the call, so we know there is a problem
                try:
                    starting_weekday_as_int = time.strptime(split_days[0], "%a").tm_wday
                    ending_weekday_as_int = time.strptime(split_days[1], "%a").tm_wday
                except ValueError:
                    self.__log_error(
                        error_message=f"Unable to parse day from {split_days}, either {split_days[0]} or "
                                      f"{split_days[1]} is in the incorrect format. "
                                      f"Please ensure the day is in the form of MON, TUE, WED, etc.. "
                                      f"Any start/stop actions this period would caused will not be performed.",
                        include_in_http_response=True,
                        fatal_error=False
                    )
                    return False

                logger.info(f"Starting day numeric representation: [{starting_weekday_as_int}] "
                            f"Ending day as numeric representation: [{ending_weekday_as_int}]")

                # TEST: What happens if it is given days like this?
                if starting_weekday_as_int <= current_date_time.weekday() <= ending_weekday_as_int:
                    logger.info(
                        f"[{current_date_time.weekday()}] is within "
                        f"[{starting_weekday_as_int}] and [{ending_weekday_as_int}]"
                    )
                    matching = True
                else:
                    logger.info(
                        f"[{current_date_time.weekday()}] is not within "
                        f"[{starting_weekday_as_int}] and [{ending_weekday_as_int}]"
                    )

            else:
                try:
                    # Evaluate as single day, e.g [MON]
                    starting_weekday_as_int = time.strptime(day_set, "%a").tm_wday
                except ValueError:
                    self.__log_error(
                        error_message=f"Unable to parse day from '{day_set}'."
                                      f"Please ensure the day is in the form of MON, TUE, WED, etc.."
                                      f"Any start/stop actions this period would caused will not be performed.",
                        include_in_http_response=True,
                        fatal_error=False
                    )

                    return False

                if current_date_time.weekday() == starting_weekday_as_int:
                    logger.info(f"Day [{current_date_time.weekday()}] is day [{starting_weekday_as_int}]")
                    matching = True
                else:
                    logger.info(f"Day [{current_date_time.weekday()}] is not day [{starting_weekday_as_int}]")

        return matching

    def __actionable_time(self, current_date_time: datetime, start_time: str = "", stop_time: str = ""):
        """
        Need to check if our given times are in one of three states:
        Before start time -> Do nothing
        After start time, before stop time -> Ensure we are started
        After stop time -> Ensure we are stopped
        Using a HH:MM, 24-HOUR clock. 00:00 is a special exception we need to check for
        If there is no start time then we don't care about starting, only stopping
        If there is no stop time then we don't care about stopping, only starting
        """
        action_type = ec2_actions.NONE
        current_time = current_date_time.time()

        # If no end time then we just have to be greater than start time to start
        logger.info(f"Evaluating current time [{current_time}] with start time: [{start_time}]"
                    f" and stop time: [{stop_time}]")
        # TODO: What happens when the stop time is late night, e.g. 23:00+ but the cron job doesn't run
        # until the next day? It will not see the the instance should have been stopped.
        # Should we have it try and stop the instance if it is not before start time, instead of no action?
        # This will increase API calls, and possibly cause a problem if the instance was manually started
        # unless we make use of the override tag
        # TODO fix the situation where it only checks start time, even if no start time is defined
        # because it drops out of the if, elif statements on the first one
        should_start = self.__should_start(current_time, start_time)
        should_stop = self.__should_stop(current_time, stop_time)

        # Putting the check for stop time first to fix the situation of not having a start time, causing it to drop
        # out of the 'if not should_start' conditional, without checking if it should stop the instance. This
        # scenario arises when no start time is defined and a stop time is defined. If the reverse happens, a start
        # time is defined, but no stop time is defined, it will pass the first first 'if should_stop' conditional
        # so we can continue evaluation. Need to make sure this is tested to catch all scenarios.
        # Do we want to keep the logic as if no start time defined, don't start, or should it mean no start time
        # defined means it should always be started during that period? I am thinking no start action should happen.
        # Past stop time
        if should_stop:
            logger.info(f"Stop time has been passed: Stopping instance")
            action_type = ec2_actions.STOP
        # Before start time
        elif not should_start:
            logger.info("Start time has not been passed. No action should be taken.")
            action_type = ec2_actions.NONE
        # Between start and stop times
        elif should_start and not should_stop:
            logger.info("Start time has been passed, stop time has not. We are between times. Starting instance")
            action_type = ec2_actions.START

        # If no start time then we just have to be greater than end time to end
        # If past start time, we should be started as long as we are not passed then stop time also

        return action_type

    def __should_start(self, current_time, start_time) -> bool:
        # Clean this up, don't do early returns
        if start_time is None or start_time == "":
            return False

        if start_time.startswith('24'):
            logger.warning("Found midnight as config. Changing start time to 00:00 to signify start of day.")
            start_time = "00:00"

        # This will give us the start time as a datetime object so we can compare times
        starting_date_time = datetime.strptime(start_time, "%H:%M")
        new_time = current_time.replace(
            hour=starting_date_time.hour,
            minute=starting_date_time.minute,
            second=0,
            microsecond=0
        )
        logger.info(f"New start time: [{new_time}]")
        # If our current time is passed our start time
        if current_time >= new_time:
            logger.info(f"Current time {current_time} is >= than period time {new_time}.")
        else:
            logger.info(f"Current time {current_time} is < than period time {new_time}.")

        return current_time >= new_time

    def __should_stop(self, current_time, stop_time):
        # No stop time, do not need to evaluate
        if stop_time is None:
            return False

        # 24:00 is not a correct value
        if stop_time.startswith('24'):
            logger.warning("Found midnight as config. Changing to 23:59 to keep in current day.")
            stop_time = "23:59"

        # This will give us the start time as a datetime object so we can compare times

        starting_date_time = datetime.strptime(stop_time, "%H:%M")

        new_time = current_time.replace(
            hour=starting_date_time.hour,
            minute=starting_date_time.minute,
            second=0,
            microsecond=0
        )

        logger.info(f"New stop time: {new_time}")

        # If our current time is passed our start time
        # BUG: This fails if our time is 00:00 as this does not represent midnight, but the start of a new day
        # TODO fix the above bug
        if current_time >= new_time:
            logger.info(f"Current time {current_time} is >= than period time {new_time}.")
        else:
            logger.info(f"Current time {current_time} is < than period time {new_time}.")

        return current_time >= new_time

    def __log_error(self, error_message: str, output_to_logger=True, include_in_http_response=False, fatal_error=False):
        if include_in_http_response:
            self.errors = error_message
        if output_to_logger:
            logger.error(error_message)
        if fatal_error:
            # TODO not implemented
            pass
