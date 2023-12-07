from typing import Sequence
from airflow.models.baseoperator import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from sqlalchemy.dialects.postgresql import insert

import pandas as pd


class InsertDataframeToPostgresOperator(BaseOperator):

    template_fields: Sequence[str] = ("dataframe",)

    def __init__(
            self,
            *,
            dataframe: pd.DataFrame,
            table_name: str,
            conn_id: str,
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.table_name = table_name
        self.conn_id = conn_id

       
    def execute(self, context):
        
        data = self.dataframe
        print(data)
        print(type(data))
        hook = PostgresHook(postgres_conn_id='postgres_dw')


        engine = hook.get_sqlalchemy_engine()
        data.to_sql(name=self.table_name, con=engine, 
                           if_exists='replace', index=False)
        
    
        