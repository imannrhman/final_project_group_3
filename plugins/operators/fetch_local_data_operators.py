from airflow.models.baseoperator import BaseOperator
from airflow.exceptions import AirflowException
from typing import Callable


class FetchLocalDataOperator(BaseOperator):

    def __init__(
            self,
            sources_data:list[str],
            converter: Callable,
            **kwargs) -> None:
        super().__init__(**kwargs)
        if not callable(converter):
            raise AirflowException("`converter` param must be callable")
        self.source_data = sources_data
        self.converter = converter

    def execute(self, 
                context):
        df_result = self.converter(self.source_data);
        return df_result;
