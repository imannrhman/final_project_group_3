from typing import Sequence
from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from numpy import dtype
import pandas as pd

class ChangeType:
   def __init__(self, column_name: str, new_type: dtype):
       self.column_name = column_name
       self.new_type = new_type

class CleanDataframeOperator(BaseOperator):

    template_fields: Sequence[str] = ("dataframe",)

    def __init__(
            self,
            *,
            dataframe: pd.DataFrame,
            drop_columns: list = [],
            drop_duplicates_columns: list = [],
            change_type_columns: list[ChangeType] = [],
            **kwargs) -> None:
        super().__init__(**kwargs)
        self.dataframe = dataframe
        self.drop_columns = drop_columns
        self.drop_duplicates_columns = drop_duplicates_columns
        self.change_type_columns = change_type_columns

    def execute(self, context):
        clean_df = self.dataframe
        if self.drop_columns :
            clean_df = clean_df.drop(columns=self.drop_columns)
       
        if self.drop_duplicates_columns:
            clean_df.drop_duplicates(keep='last', inplace=True, subset=self.drop_duplicates_columns)
        for column in self.change_type_columns:
            clean_df[column.column_name] = clean_df[column.column_name].astype(column.new_type)    
        return clean_df;
