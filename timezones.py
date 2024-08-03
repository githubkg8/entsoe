import pytz
from datetime import datetime

class TimeZoneManager():
    def __init__(self,local_timezone) -> None:
        self.local_tz=pytz.timezone(local_timezone)

    def get_utc_time(self, date: str) -> datetime:
        try:
            local_date=self.local_tz.localize(date)
            utc_time = local_date.astimezone(pytz.UTC)
            return utc_time
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")