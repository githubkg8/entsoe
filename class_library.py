import logging
import pytz
import pandas as pd
from sqlalchemy import create_engine
from datetime import datetime

from credentials import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLManager():
    '''Class to manage SQL operations'''
    def __init__(self) -> None:
        self.db_engine = create_engine(url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DATABASE}')

    def upload_sql(self,df,table_name,schema_name):
        '''Uploads a pandas dataframe to a SQL table'''
        if not df.empty:
            try:
                df.to_sql(table_name, con=self.db_engine, schema=schema_name, if_exists='append', index=False)
                return True
            except Exception as e:
                logger.error(f"Error while uploading {table_name}: {e}")
        else:
            logger.error(f"No {table_name} data got from the API")

    def read_table(self,schema_name,table_name,latest_count=0):
        '''Reads a SQL table to a pandas dataframe'''
        try:
            df_all=pd.read_sql(f'SELECT * FROM "{schema_name}".{table_name}', con=self.db_engine)
            df=df_all.tail(latest_count)
        except Exception as e:
            logger.error(f"Error while reading {schema_name} {table_name}: {e}")
        return df

    def get_last_row_element(self,schema_name,table_name,column_name):
        '''Reads a SQL table to a pandas dataframe'''
        try:
            last_timestamp=pd.read_sql(f'SELECT MAX("{column_name}") FROM "{schema_name}".{table_name}', con=self.db_engine).values[0][0]
        except Exception as e:
            logger.error(f"Error while reading {table_name} table {column_name} column's last element: {e}")
        return last_timestamp
    
    def get_column_names(self,schema_name,table_name):
        '''Reads the columns name from an SQL table to a pandas dataframe'''
        try:
            columns=pd.read_sql(f'SELECT column_name FROM information_schema.columns WHERE table_schema = \'{schema_name}\' AND table_name = \'{table_name}\'', con=self.db_engine)
        except Exception as e:
            logger.error(f"Error while reading {table_name} table columns: {e}")
        return columns

class TimeZoneManager():
    '''Class to manage UTC and local timezones'''
    def __init__(self,local_timezone) -> None:
        self.local_tz=pytz.timezone(local_timezone)
        self.utc_tz=pytz.UTC

    def get_utc_time(self, date: str) -> datetime:
        try:
            local_date=self.local_tz.localize(date)
            utc_time = local_date.astimezone(pytz.UTC)
            return utc_time
        except ValueError as e:
            raise ValueError(f"Invalid date format: {e}")
        
class EntsoeCodes:
    '''Class to store ENTSO-E codes'''
    class MarketAgreement:
        Daily = "A01"
        Weekly = "A02"
        Monthly = "A03"
        Yearly = "A04"
        Total = "A05"
        Long_term = "A06"
        Intraday = "A07"
        Hourly = "A13"
    
    class AuctionType:
        Implicit = "A01"
        Explicit = "A02"
    
    class AuctionCategory:
        Base = "A01"
        Peak = "A02"
        Off_Peak = "A03"
        Hourly = "A04"
    
    class PsrType:
        dict = {"B01" : "Biomass",
                "B02" : "Fossil_Brown_coal_Lignite",
                "B03" : "Fossil_Coal_derived_gas",
                "B04" : "Fossil_Gas",
                "B05" : "Fossil_Hard_coal",
                "B06" : "Fossil_Oil",
                "B07" : "Fossil_Oil_shale",
                "B08" : "Fossil_Peat",
                "B09" : "Geothermal",
                "B10" : "Hydro_Pumped_Storage",
                "B11" : "Hydro_Run_of_river_and_poundage",
                "B12" : "Hydro_Water_Reservoir",
                "B13" : "Marine",
                "B14" : "Nuclear",
                "B15" : "Other_renewable",
                "B16" : "Solar",
                "B17" : "Waste",
                "B18" : "Wind_Offshore",
                "B19" : "Wind_Onshore",
                "B20" : "Other"}
               
        Mixed = "A03"
        Generation = "A04"
        Load = "A05"
        Biomass = "B01"
        Fossil_Brown_coal_Lignite = "B02"
        Fossil_Coal_derived_gas = "B03"
        Fossil_Gas = "B04"
        Fossil_Hard_coal = "B05"
        Fossil_Oil = "B06"
        Fossil_Oil_shale = "B07"
        Fossil_Peat = "B08"
        Geothermal = "B09"
        Hydro_Pumped_Storage = "B10"
        Hydro_Run_of_river_and_poundage = "B11"
        Hydro_Water_Reservoir = "B12"
        Marine = "B13"
        Nuclear = "B14"
        Other_renewable = "B15"
        Solar = "B16"
        Waste = "B17"
        Wind_Offshore = "B18"
        Wind_Onshore = "B19"
        Other = "B20"
        AC_Link = "B21"
        DC_Link = "B22"
        Substation = "B23"
        Transformer = "B24"
    
    class BusinessType:
        Production = "A01"
        Consumption = "A04"
        Aggregated_energy_data = "A14"
        Balance_energy_deviation = "A19"
        General_Capacity_Information = "A25"
        Already_allocated_capacity_AAC = "A29"
        Installed_generation = "A37"
        Requested_capacity_without_price = "A43"
        System_Operator_redispatching = "A46"
        Planned_maintenance = "A53"
        Unplanned_outage = "A54"
        Minimum_possible = "A60"
        Maximum_possible = "A61"
        Internal_redispatch = "A85"
        Positive_forecast_margin = "A91"
        Negative_forecast_margin = "A92"
        Wind_generation = "A93"
        Solar_generation = "A94"
        Frequency_containment_reserve = "A95"
        Automatic_frequency_restoration_reserve = "A96"
        Manual_frequency_restoration_reserve = "A97"
        Replacement_reserve = "A98"
        Interconnector_network_evolution = "B01"
        Interconnector_network_dismantling = "B02"
        Counter_trade = "B03"
        Congestion_costs = "B04"
        Capacity_allocated_including_price = "B05"
        Auction_revenue = "B07"
        Total_nominated_capacity = "B08"
        Net_position = "B09"
        Congestion_income = "B10"
        Production_unit = "B11"
        Area_Control_Error = "B33"
        Offer = "B74"
        Need = "B75"
        Procured_capacity = "B95"
        Shared_Balancing_Reserve_Capacity = "C22"
        Share_of_reserve_capacity = "C23"
        Actual_reserve_capacity = "C24"
    
    class ProcessType:
        Day_ahead = "A01"
        Intra_day_incremental = "A02"
        Realised = "A16"
        Intraday_total = "A18"
        Week_ahead = "A31"
        Month_ahead = "A32"
        Year_ahead = "A33"
        Synchronisation_process = "A39"
        Intraday_process = "A40"
        Replacement_reserve = "A46"
        Manual_frequency_restoration_reserve = "A47"
        Automatic_frequency_restoration_reserve = "A51"
        Frequency_containment_reserve = "A52"
        Frequency_restoration_reserve = "A56"
        Scheduled_activation_mFRR = "A60"
        Direct_activation_mFRR = "A61"
        Central_Selection_aFRR = "A67"
        Local_Selection_aFRR = "A68"
    
    class DocStatus:
        Intermediate = "A01"
        Final = "A02"
        Active = "A05"
        Cancelled = "A09"
        Withdrawn = "A13"
        Estimated = "X01"
    
    class DocumentType:
        Finalised_schedule = "A09"
        Aggregated_energy_data_report = "A11"
        Acquiring_system_operator_reserve_schedule = "A15"
        Bid_document = "A24"
        Allocation_result_document = "A25"
        Capacity_document = "A26"
        Agreed_capacity = "A31"
        Reserve_bid_document = "A37"
        Reserve_allocation_result_document = "A38"
        Price_Document = "A44"
        Estimated_Net_Transfer_Capacity = "A61"
        Redispatch_notice = "A63"
        System_total_load = "A65"
        Installed_generation_per_type = "A68"
        Wind_and_solar_forecast = "A69"
        Load_forecast_margin = "A70"
        Generation_forecast = "A71"
        Reservoir_filling_information = "A72"
        Actual_generation = "A73"
        Wind_and_solar_generation = "A74"
        Actual_generation_per_type = "A75"
        Load_unavailability = "A76"
        Production_unavailability = "A77"
        Transmission_unavailability = "A78"
        Offshore_grid_infrastructure_unavailability = "A79"
        Generation_unavailability = "A80"
        Contracted_reserves = "A81"
        Accepted_offers = "A82"
        Activated_balancing_quantities = "A83"
        Activated_balancing_prices = "A84"
        Imbalance_prices = "A85"
        Imbalance_volume = "A86"
        Financial_situation = "A87"
        Cross_border_balancing = "A88"
        Contracted_reserve_prices = "A89"
        Interconnection_network_expansion = "A90"
        Counter_trade_notice = "A91"
        Congestion_costs = "A92"
        DC_link_capacity = "A93"
        Non_EU_allocations = "A94"
        Configuration_document = "A95"
        Flow_based_allocations = "B11"
        Aggregated_netted_external_TSO_schedule_document = "B17"
        Bid_Availability_Document = "B45"
    
    class FlowDirection:
        Up = "A01"
        Down = "A02"
        Symmetric = "A03"
    
    class StandardMarketProduct:
        Standard = "A01"
        Specific = "A02"
        Integrated_process = "A03"
        Local = "A04"
        Standard_mFRR_DA = "A05"
        Standard_mFRR_SA_DA = "A07"
    
    class ImbalancePriceCategory:
        Excess_balance = "A04"
        Insufficient_balance = "A05"
        Average_bid_price = "A06"
        Single_marginal_bid_price = "A07"
        Cross_border_marginal_price = "A08"
    
    class PriceDescriptorType:
        Scarcity = "A01"
        Incentive = "A02"
        Financial_neutrality = "A03"
    
    class FinancialPriceDirection:
        Expenditure = "A01"
        Income = "A02"

    class Areas:
        ''' BZN-Bidding Zone | BZA-Bidding Zone Aggregation | CTA-Control Area | MBA-Market Balance Area| IBA-Imbalance Area | IPA-Imbalance Price Area |
            LFA-Load Frequency Control Area | LFB-Load Frequency Control Block | REG-Region | SCA-Scheduling Area | SNA-Synchronous Area'''
        dict={
            "HUN" : "10YHU-MAVIR----U",
            "GER" : "10Y1001A1001A82H",
        }
    
    class CCGTs:
        dict={
            "HUN" : ['CSP_GT1','CSP_GT2','CSP_ST','GÖNYÜ_gép1','DG3_gép7','DG3_gép8','KF_GT','KI_GTST'],
            "GER" : [],
        }