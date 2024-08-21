import requests
import logging
import pandas as pd
import numpy as np
import zipfile
import io

from bs4 import BeautifulSoup
from datetime import datetime, timedelta

from credentials import ENTSOE_TOKEN
from class_library import EntsoeCodes
from class_library import TimeZoneManager
from class_library import SQLManager


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataManager():
    def __init__(self,schema,local_timezone) -> None:
        self.entsoe_codes=EntsoeCodes()
        self.timezone_manager=TimeZoneManager(local_timezone)
        self.sql_manager=SQLManager()
        self.data_start_date=datetime(2019,12,31,23,0)
        self.schema_name=schema
        self.UTC_column="UTC"
        self.base_url=f"https://web-api.tp.entsoe.eu/api?securityToken={ENTSOE_TOKEN}"

    def __get_entsoe_response(self,params):
        '''Basic function to get any response from ENTSO-E API with given parameters'''
        try:
            response = requests.get(self.base_url, params=params)
            response.raise_for_status()

            # HANDLE ZIP FILES
            if response.headers['Content-Type'] == 'application/zip':
                logger.info("Response is a ZIP file. Extracting XML files...")
                with zipfile.ZipFile(io.BytesIO(response.content)) as zipped:
                    xml_filename = None
                    for file_info in zipped.infolist():
                        if file_info.filename.endswith('.xml'):
                            xml_filename = file_info.filename
                            break

                    if xml_filename:
                        with zipped.open(xml_filename) as xml_file:
                            response = requests.models.Response()
                            response._content = xml_file.read()
                            response.status_code = 200
                            response.headers = {'Content-Type': 'application/xml'}
                            logger.info(f"Extracted XML file: {xml_filename}")
                    else:
                        raise ValueError("No XML file found in the ZIP archive")

        except requests.exceptions.HTTPError:
            soup = BeautifulSoup(response.text, 'xml')
            logger.error(f"ENTSO-E API CALL Error: {response.status_code}, {soup.find('Reason').find('text').text}")
        return response

    def __upload_sql(self,df,table_name,periodStart_localtz,periodEnd_localtz):
        '''Uploads the dataframe to the SQL table'''
        try:
            success=self.sql_manager.upload_sql(df,table_name,self.schema_name)
            if success:
                logger.info(f"{table_name} refreshed successfully! ({periodStart_localtz.strftime('%Y-%m-%d')} - {(periodEnd_localtz+timedelta(-1)).strftime('%Y-%m-%d')})")
                return "Success"
        except Exception as e:
            logger.error(f"Error while refreshing {table_name}: {e}")
            return f"Error: {e}"

    def __get_power_prices(self,periodStart,periodEnd):
        '''Get the day ahead power prices for Hungary, UTC timezone, fromat: YYYYMMDDhhmm'''

        params={
            "documentType" : self.entsoe_codes.DocumentType.Price_Document,
            "in_Domain" : self.entsoe_codes.Areas.MAVIR,
            "out_Domain" : self.entsoe_codes.Areas.MAVIR,
            "periodStart" : periodStart,
            "periodEnd" : periodEnd
            }

        response=self.__get_entsoe_response(params)
        soup=BeautifulSoup(response.text, 'xml')

        try:
            days= [datetime.strptime(day.text.split("T")[0],"%Y-%m-%d") for period in soup.find_all('Period') for day in period.find_all('end')]
            prices=[float(price.getText()) for price in soup.find_all('price.amount')]

            datetimes_utc = []
            datetimes_local = []
            start_date = self.timezone_manager.get_utc_time(days[0])
            for hour in range(len(prices)):
                datetimes_utc.append(start_date + timedelta(hours=hour))
                datetimes_local.append(start_date.astimezone(self.timezone_manager.local_tz) + timedelta(hours=hour))
        except Exception as e:
            logger.error(f"Error while getting power prices: {soup.find('Reason').find('text').text}")
            prices = []
            datetimes_utc = []
            datetimes_local = []

            with open(f'C:\\Users\\Admin\\Projects\\entso-e\\troubleshoot\\power_prices_{periodStart}-{periodEnd}_troubleshoot.xml', 'w') as f:
                f.write(soup.prettify())

        df = pd.DataFrame({'UTC': datetimes_utc, 'local_datetime': datetimes_local, 'DA_price': prices})
        return df
    
    def __get_balancing_energy(self,periodStart,periodEnd):
        '''Get the activated balancing energy for Hungary in MW, UTC timezone, fromat: YYYYMMDDhhmm'''
        # DOMESTIC ACTIVATED BALANCING ENERGY
        params={
            "documentType" : self.entsoe_codes.DocumentType.Activated_balancing_quantities,
            "controlArea_Domain" : self.entsoe_codes.Areas.MAVIR,
            "periodStart" : periodStart,
            "periodEnd" : periodEnd
            }

        response=self.__get_entsoe_response(params)
        soup=BeautifulSoup(response.text, 'xml')
        
        try:
            RESOLUTION = timedelta(minutes=int(soup.find('resolution').getText().split('T')[1].split('M')[0]))
            period_start_date = datetime.strptime(soup.find('period.timeInterval').find('start').getText(), '%Y-%m-%dT%H:%MZ') 
            period_end_date = datetime.strptime(soup.find('period.timeInterval').find('end').getText(), '%Y-%m-%dT%H:%MZ')  

            datetimes_utc_wo_tz = [period_start_date + RESOLUTION * i for i in range(int((period_end_date - period_start_date) / RESOLUTION))]
            datetimes_utc=[time.replace(tzinfo=self.timezone_manager.utc_tz) for time in datetimes_utc_wo_tz]
            datetimes_local = []
            for quarter_hour in datetimes_utc:
                datetimes_local.append(quarter_hour.astimezone(self.timezone_manager.local_tz))

            afrr_down = np.array([-4*int(activated_energy.find('quantity').getText()) for time_series in soup.find_all('TimeSeries')
                            for period in time_series.find_all('Period')
                            for activated_energy in period.find_all('Point')
                            if time_series.find('businessType').getText() == self.entsoe_codes.BusinessType.Automatic_frequency_restoration_reserve
                            and time_series.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Down])

            afrr_up = np.array([4*int(activated_energy.find('quantity').getText()) for time_series in soup.find_all('TimeSeries')
                            for period in time_series.find_all('Period')
                            for activated_energy in period.find_all('Point')
                            if time_series.find('businessType').getText() == self.entsoe_codes.BusinessType.Automatic_frequency_restoration_reserve
                            and time_series.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Up])

            mfrr_down = np.array([-4*int(activated_energy.find('quantity').getText()) for time_series in soup.find_all('TimeSeries')
                            for period in time_series.find_all('Period')
                            for activated_energy in period.find_all('Point')
                            if time_series.find('businessType').getText() == self.entsoe_codes.BusinessType.Manual_frequency_restoration_reserve
                            and time_series.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Down])

            mfrr_up = np.array([4*int(activated_energy.find('quantity').getText()) for time_series in soup.find_all('TimeSeries')
                            for period in time_series.find_all('Period')
                            for activated_energy in period.find_all('Point')
                            if time_series.find('businessType').getText() == self.entsoe_codes.BusinessType.Manual_frequency_restoration_reserve
                            and time_series.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Up])
            
        except Exception as e:
            logger.error(f"Error while getting activated balancing energy: {e} {soup.find('Reason').find('text').text}")
            afrr_down = np.zeros(len(datetimes_utc))
            mfrr_down = np.zeros(len(datetimes_utc))
            afrr_up = np.zeros(len(datetimes_utc))
            mfrr_up = np.zeros(len(datetimes_utc))

            with open(f'C:\\Users\\Admin\\Projects\\entso-e\\troubleshoot\\activated_balancing_energy_{periodStart}-{periodEnd}_troubleshoot.xml', 'w') as f:
                f.write(soup.prettify())
        
        # check if the arrays have the same length, if not add zeros
        arrays_dict = {'afrr_down': afrr_down, 'mfrr_down': mfrr_down,'afrr_up': afrr_up, 'mfrr_up': mfrr_up}
        len_utc=len(datetimes_utc)
        for name, array in arrays_dict.items():
            if len_utc > len(array):
                logger.warning(f"{name} length: {len(array)}, adding {len_utc - len(array)} zeros")
                arrays_dict[name] = np.pad(array, (0, len_utc - len(array)), mode='constant')
        afrr_down, mfrr_down, afrr_up, mfrr_up = arrays_dict['afrr_down'], arrays_dict['mfrr_down'], arrays_dict['afrr_up'], arrays_dict['mfrr_up']


        # TOTAL IMBALANCE VOLUME
        params={
            "documentType" : self.entsoe_codes.DocumentType.Imbalance_volume,
            "controlArea_Domain" : self.entsoe_codes.Areas.MAVIR,
            "periodStart" : periodStart,
            "periodEnd" : periodEnd
            }

        response=self.__get_entsoe_response(params)
        soup=BeautifulSoup(response.text, 'xml')
        
        try:
            RESOLUTION=timedelta(minutes=int(soup.find('resolution').getText().split('T')[1].split('M')[0]))
            period_start = datetime.strptime(soup.find('period.timeInterval').find('start').getText(),'%Y-%m-%dT%H:%MZ')
            period_end=datetime.strptime(soup.find('period.timeInterval').find('end').getText(),'%Y-%m-%dT%H:%MZ')

            total_imbalance_down = {}
            for ts in soup.find_all('TimeSeries'):
                if ts.find('businessType').getText() == self.entsoe_codes.BusinessType.Balance_energy_deviation and ts.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Down:
                    for period in ts.find_all('Period'):
                        for point in period.find_all('Point'):
                            total_imbalance_down[datetime.strptime(period.find('start').getText(),'%Y-%m-%dT%H:%MZ')+(int(point.find('position').getText())-1)*RESOLUTION]=-4*int(point.find('quantity').getText())
            
            total_imbalance_up = {}
            for ts in soup.find_all('TimeSeries'):
                if ts.find('businessType').getText() == self.entsoe_codes.BusinessType.Balance_energy_deviation and ts.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Up:                   
                    for period in ts.find_all('Period'):
                        for point in period.find_all('Point'):
                            total_imbalance_up[datetime.strptime(period.find('start').getText(),'%Y-%m-%dT%H:%MZ')+(int(point.find('position').getText())-1)*RESOLUTION]=4*int(point.find('quantity').getText())

            time_index = period_start
            while time_index < period_end:
                if time_index not in total_imbalance_down:
                    total_imbalance_down[time_index] = 0
                if time_index not in total_imbalance_up:
                    total_imbalance_up[time_index] = 0
                time_index += RESOLUTION
            
            igcc_down= np.minimum(0,np.array([total_imbalance_down[time] for time in datetimes_utc_wo_tz]) - afrr_down - mfrr_down)
            igcc_up = np.maximum(0,np.array([total_imbalance_up[time] for time in datetimes_utc_wo_tz]) - afrr_up - mfrr_up)

        except Exception as e:
            logger.error(f"Error while getting total imbalance volume: {e}")
            igcc_down = np.zeros(len(datetimes_utc))
            igcc_up = np.zeros(len(datetimes_utc))
            
            with open(f'C:\\Users\\Admin\\Projects\\entso-e\\troubleshoot\\total_imbalance_{periodStart}-{periodEnd}_troubleshoot.xml', 'w') as f:
                f.write(soup.prettify())


        # PRICES OF ACTIVATED DOMESTIC BALANCING ENERGY
        
        params={
            "documentType" : self.entsoe_codes.DocumentType.Activated_balancing_prices,
            "controlArea_Domain" : self.entsoe_codes.Areas.MAVIR,
            "businessType" : self.entsoe_codes.BusinessType.Automatic_frequency_restoration_reserve,
            "periodStart" : periodStart,
            "periodEnd" : periodEnd
            }

        response=self.__get_entsoe_response(params)
        soup=BeautifulSoup(response.text, 'xml')

        try:
            price_down_dict={point.find("position").getText() : point.find("activation_Price.amount").getText() for time_series in soup.find_all('TimeSeries') for point in time_series.find_all('Point') if time_series.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Down}
            price_up_dict={point.find("position").getText() : point.find("activation_Price.amount").getText() for time_series in soup.find_all('TimeSeries') for point in time_series.find_all('Point') if time_series.find('flowDirection.direction').getText() == self.entsoe_codes.FlowDirection.Up}
        except Exception as e:
            logger.error(f"Error while getting prices of activated AFRR balancing energy: {e}")
            price_down_dict = {}
            price_up_dict = {}

        # check if the dictionaries have all values in range len_utc, if not add zeros
        price_down = np.array([float(price_down_dict.get(str(i+1), 0)) for i in range(len(datetimes_utc))])
        price_up = np.array([float(price_up_dict.get(str(i+1), 0)) for i in range(len(datetimes_utc))])

        df = pd.DataFrame({'UTC': datetimes_utc, 'local_datetime': datetimes_local, 'down_afrr': afrr_down, 'down_igcc': igcc_down, 'down_mfrr': mfrr_down, 'up_afrr': afrr_up, 'up_igcc': igcc_up, 'up_mfrr': mfrr_up, 'down_price': price_down, 'up_price': price_up})
        return df

    def __get_fuelmix(self,periodStart,periodEnd):
        '''Get the day ahead power prices for Hungary, UTC timezone, fromat: YYYYMMDDhhmm'''

        params={
            "documentType" : self.entsoe_codes.DocumentType.Actual_generation_per_type,
            "in_Domain" : self.entsoe_codes.Areas.MAVIR,
            "ProcessType" : self.entsoe_codes.ProcessType.Realised,
            "periodStart" : periodStart,
            "periodEnd" : periodEnd,
            }

        response=self.__get_entsoe_response(params)
        soup=BeautifulSoup(response.text, 'xml')

        try:
            days = [datetime.strptime(day.text.split("T")[0],"%Y-%m-%d") for period in soup.find_all('Period') for day in period.find_all('end')]
            RESOLUTION=timedelta(minutes=int(soup.find('resolution').getText().split('T')[1].split('M')[0]))
            # response is with codes, need to convert to readable format
            # get db column names (source types)
            response_production_per_type = {ts.find('psrType').get_text(): [int(quantity.get_text()) for quantity in ts.find_all("quantity")] for ts in soup.find_all('TimeSeries')}
            response_production_per_type = {self.entsoe_codes.PsrType.dict[key]: response_production_per_type[key] for key in response_production_per_type.keys()}
            response_ts_max_length = max([len(response_production_per_type[key]) for key in response_production_per_type.keys()])
            db_source_types = [row.column_name for index, row in self.sql_manager.get_column_names(self.schema_name, 'fuelmix')[2:].iterrows()]

            # filling with 0s if source type not covering the whole period
            for time_series in soup.find_all('TimeSeries'):
                start_source_series=datetime.strptime(time_series.find('timeInterval').find('start').get_text(), '%Y-%m-%dT%H:%MZ')
                end_source_series=datetime.strptime(time_series.find('timeInterval').find('end').get_text(), '%Y-%m-%dT%H:%MZ')
                source_type = self.entsoe_codes.PsrType.dict[time_series.find('psrType').get_text()]
                
                if len(time_series.find_all('quantity')) < response_ts_max_length:
                    if len(response_production_per_type[source_type]) < response_ts_max_length:
                        logger.warning(f"Source type {source_type} does not cover the whole period! ({start_source_series} - {end_source_series}) Filling data with 0s...")
                        response_production_per_type[source_type]=np.zeros(response_ts_max_length)
                    original_response = [int(quantity.get_text()) for quantity in time_series.find_all('quantity')]
                    i=0
                    for quantity in original_response:
                        index=(start_source_series - datetime.strptime(periodStart, '%Y%m%d%H%M')) // RESOLUTION + i
                        response_production_per_type[source_type][index] = quantity
                        i+=1

                    with open(f'C:\\Users\\Admin\\Projects\\entso-e\\troubleshoot\\fuelmix_{source_type}_{periodStart}-{periodEnd}_troubleshoot.xml', 'w') as f:
                        f.write(str(response_production_per_type[source_type]))

            # fill with 0s psr_types that are missing from the response
            for source_type in db_source_types:
                if source_type not in response_production_per_type.keys():
                    response_production_per_type[source_type] = [0 for _ in range(len(response_production_per_type[next(iter(response_production_per_type))]))]

            datetimes_utc = []
            datetimes_local = []
            start_date = self.timezone_manager.get_utc_time(days[0])
            for period in range(len(response_production_per_type[next(iter(response_production_per_type))])):
                datetimes_utc.append(start_date + period*RESOLUTION)
                datetimes_local.append(start_date.astimezone(self.timezone_manager.local_tz) + period*RESOLUTION)
        except Exception as e:
            logger.error(f"Error while getting fuelmix: {soup.find('Reason').find('text').text}")
            response_production_per_type = []
            datetimes_utc = []
            datetimes_local = []

            with open(f'C:\\Users\\Admin\\Projects\\entso-e\\troubleshoot\\fuelmix_{periodStart}-{periodEnd}_troubleshoot.xml', 'w') as f:
                f.write(soup.prettify())

        df = pd.DataFrame({'UTC': datetimes_utc, 'local_datetime': datetimes_local, **response_production_per_type})
        return df
    
    def __get_actual_total_load(self,periodStart,periodEnd):
        '''Get the actual total load for Hungary, UTC timezone, fromat: YYYYMMDDhhmm'''

        params={
            "documentType" : self.entsoe_codes.DocumentType.System_total_load,
            "ProcessType" : self.entsoe_codes.ProcessType.Realised,
            "OutBiddingZone_Domain" : self.entsoe_codes.Areas.MAVIR,
            "periodStart" : periodStart,
            "periodEnd" : periodEnd
            }
        
        response=self.__get_entsoe_response(params)
        soup=BeautifulSoup(response.text, 'xml')

        try:
            days = [datetime.strptime(day.text.split("T")[0],"%Y-%m-%d") for period in soup.find_all('Period') for day in period.find_all('end')]
            RESOLUTION=timedelta(minutes=int(soup.find('resolution').getText().split('T')[1].split('M')[0]))
            total_load=[int(load.getText()) for load in soup.find_all('quantity')]
            datetimes_utc = []
            datetimes_local = []
            start_date = self.timezone_manager.get_utc_time(days[0])
            for period in range(len(total_load)):
                datetimes_utc.append(start_date + period*RESOLUTION)
                datetimes_local.append(start_date.astimezone(self.timezone_manager.local_tz) + period*RESOLUTION)
        except Exception as e:
            logger.error(f"Error while getting actual total load: {soup.find('Reason').find('text').text}")
            total_load = []
            datetimes_utc = []
            datetimes_local = []

            with open(f'C:\\Users\\Admin\\Projects\\entso-e\\troubleshoot\\actual_total_load_{periodStart}-{periodEnd}_troubleshoot.xml', 'w') as f:
                f.write(soup.prettify())

        df = pd.DataFrame({'UTC': datetimes_utc, 'local_datetime': datetimes_local, 'Actual_load': total_load})
        return df

    def update_power_prices(self):
        '''Refreshes the power prices from the last updated date until days ahead'''

        # Get the last timestamp from the database
        # Set the periodStart and periodEnd for day ahead
        # Convert the local timezone to UTC
        table_name='power_price'

        last_timestamp=self.sql_manager.get_last_row_element(self.schema_name,table_name,self.UTC_column)
        if last_timestamp is None:
            last_timestamp=self.data_start_date
            logger.warning(f"No data found: {table_name}, last_timestamp set to: {last_timestamp}")
        day_ahead_end = datetime.now() + timedelta(days=2)
        periodStart_localtz = datetime(last_timestamp.year,last_timestamp.month,last_timestamp.day,0,0) + timedelta(days=1) 
        periodEnd_localtz = datetime(day_ahead_end.year,day_ahead_end.month,day_ahead_end.day,0,0) 

        periodStart=self.timezone_manager.get_utc_time(periodStart_localtz).strftime('%Y%m%d%H%M')
        periodEnd=self.timezone_manager.get_utc_time(periodEnd_localtz).strftime('%Y%m%d%H%M')

        if periodStart != periodEnd:
            # Get the power prices from ENTSO-E API, returns a pd dataframe
            # Maximum period is 1 year, if the period is longer, it is divided into 1 day periods
            if periodEnd_localtz - periodStart_localtz < timedelta(days=365):
                df_da_prices=self.__get_power_prices(periodStart,periodEnd)
                self.__upload_sql(df_da_prices,table_name,periodStart_localtz,periodEnd_localtz)

            else:
                for day in range((periodEnd_localtz - periodStart_localtz).days):
                    periodStart_i = periodStart_localtz+ timedelta(days=day)
                    periodEnd_i = periodStart_localtz + timedelta(days=day+1)
                    periodStart=self.timezone_manager.get_utc_time(periodStart_i).strftime('%Y%m%d%H%M')
                    periodEnd=self.timezone_manager.get_utc_time(periodEnd_i).strftime('%Y%m%d%H%M')
                    
                    df_da_prices=self.__get_power_prices(periodStart,periodEnd)
                    self.__upload_sql(df_da_prices,table_name,periodStart_i,periodEnd_i)                

        else:
            logger.info(f"power_prices are up to date! ({(periodStart_localtz+timedelta(-1)).strftime('%Y-%m-%d')})")
            return "No new data to update"

    def update_activated_balancing_energy(self):
        '''Refreshes the activated balancing energy from the last updated date until recent data'''

        # Get the last timestamp from the database
        # Set the periodStart and periodEnd for recent data
        # Convert the local timezone to UTC
        table_name='activated_balancing_energy'

        last_timestamp=self.sql_manager.get_last_row_element(self.schema_name,table_name,self.UTC_column)
        if last_timestamp is None:
            last_timestamp=self.data_start_date
            logger.warning(f"No data found: {table_name}, last_timestamp set to: {last_timestamp}")
        today = datetime.now()
        periodStart_localtz = datetime(last_timestamp.year,last_timestamp.month,last_timestamp.day,0,0) + timedelta(days=1)
        periodEnd_localtz = datetime(today.year,today.month,today.day,0,0)

        #  EACH DAY IS RUNNED SEPARATELY (if incorrect mfrr data, fixed by adding 0s to the end of the array) 
        if periodStart_localtz != periodEnd_localtz:
            for day in range((periodEnd_localtz - periodStart_localtz).days):
                periodStart_i = periodStart_localtz+ timedelta(days=day)
                periodEnd_i = periodStart_localtz + timedelta(days=day+1)

                periodStart=self.timezone_manager.get_utc_time(periodStart_i).strftime('%Y%m%d%H%M')
                periodEnd=self.timezone_manager.get_utc_time(periodEnd_i).strftime('%Y%m%d%H%M')
            
                df_abe=self.__get_balancing_energy(periodStart,periodEnd)
                self.__upload_sql(df_abe,table_name,periodStart_i,periodEnd_i)
                
        else:
                logger.info(f"activated_balancing_prices are up to date! ({(periodStart_localtz+timedelta(-1)).strftime('%Y-%m-%d')})")
                return "No new data to update"
    
    def update_fuelmix(self):
        '''Refreshes the fuelmix data from the last updated date until recent data'''

        # Get the last timestamp from the database
        # Set the periodStart and periodEnd for recent data
        # Convert the local timezone to UTC
        table_name='fuelmix'

        last_timestamp=self.sql_manager.get_last_row_element(self.schema_name,table_name,self.UTC_column)
        if last_timestamp is None:
            last_timestamp=self.data_start_date
            logger.warning(f"No data found: {table_name}, last_timestamp set to: {last_timestamp}")
        today = datetime.now()
        periodStart_localtz = datetime(last_timestamp.year,last_timestamp.month,last_timestamp.day,0,0) + timedelta(days=1)
        periodEnd_localtz = datetime(today.year,today.month,today.day,0,0)

        if periodStart_localtz != periodEnd_localtz:
            for day in range((periodEnd_localtz - periodStart_localtz).days):
                periodStart_i = periodStart_localtz+ timedelta(days=day)
                periodEnd_i = periodStart_localtz + timedelta(days=day+1)

                periodStart=self.timezone_manager.get_utc_time(periodStart_i).strftime('%Y%m%d%H%M')
                periodEnd=self.timezone_manager.get_utc_time(periodEnd_i).strftime('%Y%m%d%H%M')

                df_fuelmix=self.__get_fuelmix(periodStart,periodEnd)
                self.__upload_sql(df_fuelmix,table_name,periodStart_i,periodEnd_i)
        else:
            logger.info(f"fuelmix is up to date! ({(periodStart_localtz+timedelta(-1)).strftime('%Y-%m-%d')})")
            return "No new data to update"
        
    def update_actual_total_load(self):
        '''Refreshes the actual total load data from the last updated date until recent data'''

        # Get the last timestamp from the database
        # Set the periodStart and periodEnd for recent data
        # Convert the local timezone to UTC
        table_name='actual_total_load'

        last_timestamp=self.sql_manager.get_last_row_element(self.schema_name,table_name,self.UTC_column)
        if last_timestamp is None:
            last_timestamp=self.data_start_date
            logger.warning(f"No data found: {table_name}, last_timestamp set to: {last_timestamp}")
        today = datetime.now()
        
        periodStart_localtz = datetime(last_timestamp.year,last_timestamp.month,last_timestamp.day,0,0) + timedelta(days=1)
        periodEnd_localtz = datetime(today.year,today.month,today.day,0,0)
        if periodStart_localtz != periodEnd_localtz:
            for day in range((periodEnd_localtz - periodStart_localtz).days):
                periodStart_i = periodStart_localtz+ timedelta(days=day)
                periodEnd_i = periodStart_localtz + timedelta(days=day+1)

                periodStart=self.timezone_manager.get_utc_time(periodStart_i).strftime('%Y%m%d%H%M')
                periodEnd=self.timezone_manager.get_utc_time(periodEnd_i).strftime('%Y%m%d%H%M')

                df_atl=self.__get_actual_total_load(periodStart,periodEnd)
                self.__upload_sql(df_atl,table_name,periodStart_i,periodEnd_i)
        else:
            logger.info(f"actual_total_load is up to date! ({(periodStart_localtz+timedelta(-1)).strftime('%Y-%m-%d')})")
            return "No new data to update"


''' 
- Get big power plants schedule
- Get capacity prices

'''