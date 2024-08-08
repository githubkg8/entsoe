import logging
from sqlalchemy import create_engine
import pandas as pd

from credentials import POSTGRES_USER, POSTGRES_PASSWORD, POSTGRES_DATABASE


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SQLManager():
    def __init__(self) -> None:
        self.db_engine = create_engine(url = f'postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@localhost:5432/{POSTGRES_DATABASE}')

    def upload_sql(self,df,table_name,schema_name):
        '''Uploads a pandas dataframe to a SQL table'''
        try:
            df.to_sql(table_name, con=self.db_engine, schema=schema_name, if_exists='append', index=False)
            return True
        except Exception as e:
            logger.error(f"Error while uploading {table_name}: {e}")

    def read_table(self,schema_name,table_name,latest_count=0):
        '''Reads a SQL table to a pandas dataframe'''
        try:
            df_all=pd.read_sql(f'SELECT * FROM "{schema_name}".{table_name}', con=self.db_engine)
            df=df_all.tail(latest_count)
        except Exception as e:
            logger.error(f"Error while reading {schema_name} {table_name}: {e}")
        return df

    def get_last_column_element(self,schema_name,table_name,column_name):
        '''Reads a SQL table to a pandas dataframe'''
        try:
            last_timestamp=pd.read_sql(f'SELECT MAX("{column_name}") FROM "{schema_name}".{table_name}', con=self.db_engine).values[0][0]
        except Exception as e:
            logger.error(f"Error while reading {table_name} table {column_name} column's last element: {e}")
        return last_timestamp
    

        