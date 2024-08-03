import requests
import logging
import pandas as pd

from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from credentials import ENTSOE_TOKEN, POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE
from entsoe_codes import EntsoeCodes
from timezones import TimeZoneManager
from sqlalchemy import create_engine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataManager():
    def __init__(self,local_timezone) -> None:
        self.entsoe_codes=EntsoeCodes()
        self.timezone_manager=TimeZoneManager(local_timezone)
        self.db_engine = create_engine(url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DATABASE}')
        self.base_url=f"https://web-api.tp.entsoe.eu/api?securityToken={ENTSOE_TOKEN}"

    def get_entsoe_response(self,params):
        '''Basic function to get any response from ENTSO-E API with given parameters'''

        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()
            logger.info(f"ENTSO-E API CALL Success! Response code: {response.status_code}")
        except requests.exceptions.HTTPError:
            soup = BeautifulSoup(response.text, 'xml')
            logger.error(f"ENTSO-E API CALL Error: {response.status_code}, {soup.find('Reason').find('text').text}")
        return response

    def upload_sql(self,df,table_name,schema):
        '''Uploads a pandas dataframe to a SQL table'''

        try:
            df.to_sql(table_name, con=self.db_engine, schema=schema, if_exists='append', index=False)
            logger.info(f"{table_name} uploaded successfully!")
        except Exception as e:
            logger.error(f"Error while uploading {table_name}: {e}")

    def get_power_prices(self,periodStart,periodEnd):
        '''Get the day ahead power prices for Hungary, UTC timezone, fromat: YYYYMMDDhhmm'''

        params={
            "documentType" : self.entsoe_codes.DocumentType.Price_Document,
            "in_Domain" : self.entsoe_codes.Areas.MAVIR,
            "out_Domain" : self.entsoe_codes.Areas.MAVIR,
            "periodStart" : periodStart,
            "periodEnd" : periodEnd
            }

        response=self.get_entsoe_response(params)
        soup=BeautifulSoup(response.text, 'xml')

        days= [datetime.strptime(day.text.split("T")[0],"%Y-%m-%d") for period in soup.find_all('Period') for day in period.find_all('end')]
        prices=[float(price.getText()) for price in soup.find_all('price.amount')]

        datetimes_utc = []
        datetimes_local = []
        start_date = self.timezone_manager.get_utc_time(days[0])
        for hour in range(len(prices)):
            datetimes_utc.append(start_date + timedelta(hours=hour))
            datetimes_local.append(start_date.astimezone(self.timezone_manager.local_tz) + timedelta(hours=hour))

        df = pd.DataFrame({'UTC': datetimes_utc, 'local_datetime': datetimes_local, 'DA_price': prices})
        return df
    
    def update_power_prices(self):
        '''Refreshes the power prices from the last updated date until days ahead'''

        # Get the last timestamp from the database 
        # Set the periodStart and periodEnd for day ahead
        # Convert the local timezone to UTC
        last_timestamp=pd.read_sql('SELECT MAX("UTC") FROM "HUN".power_price', con=self.db_engine).values[0][0]
        day_ahead_end = datetime.now() + timedelta(days=2)
        periodStart_localtz = datetime(last_timestamp.year,last_timestamp.month,last_timestamp.day,0,0) + timedelta(days=1)
        periodEnd_localtz = datetime(day_ahead_end.year,day_ahead_end.month,day_ahead_end.day,0,0)

        periodStart=self.timezone_manager.get_utc_time(periodStart_localtz).strftime('%Y%m%d%H%M')
        periodEnd=self.timezone_manager.get_utc_time(periodEnd_localtz).strftime('%Y%m%d%H%M')

        if periodStart != periodEnd:
            # Get the power prices from ENTSO-E API, returns a pd dataframe
            df_da_prices=self.get_power_prices(periodStart,periodEnd)

            # Upload the data to the SQL table
            try:
                table_name='power_price'
                schema_name='HUN'
                self.upload_sql(df_da_prices,table_name,schema_name)
                logger.info(f"power_prices refreshed successfully! ({periodStart_localtz.strftime('%Y-%m-%d')} - {(periodEnd_localtz+timedelta(-1)).strftime('%Y-%m-%d')})")
                return "Success"
            except Exception as e: 
                logger.error(f"Error while refreshing power_prices: {e}")
                return f"Error: {e}"
        else:
            logger.info(f"power_prices are up to date! ({(periodStart_localtz+timedelta(-1)).strftime('%Y-%m-%d')})")
            return "No new data to update"