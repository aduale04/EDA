import yaml
from sqlalchemy import create_engine
import pandas as pd
import psycopg2
def load_credentials(file_path='credentials.yaml'):
    with open(file_path, 'r') as f:
        credentials = yaml.load(f, Loader=yaml.FullLoader)
    return credentials

class RDSDatabaseConnector:
    def __init__(self, credentials):
        self.engine = self.create_db_engine(credentials)

    def create_db_engine(self, credentials):
        db_url = f"postgresql://{credentials['RDS_USER']}:{credentials['RDS_PASSWORD']}@{credentials['RDS_HOST']}:{credentials['RDS_PORT']}/{credentials['RDS_DATABASE']}"
        engine = create_engine(db_url)
        return engine

    def fetch_data(self, table_name='loan_payments'):
        query = f"SELECT * FROM {table_name}"
        with self.engine.connect() as connection:
            dataframe = pd.read_sql_query(query, connection)
        return dataframe

    def store_data(self, dataframe, path='data.csv'):
        dataframe.to_csv(path, index=False)
        print(f"data stored in {path}")

    def open_dataframe(self, path):
        df = pd.read_csv(path)
        print("stored CSV in df")
        return df
