import pandas as pd #untuk manipulasi data, version=2.1.0
import polars as pl #untuk manipulasi data, handle file avro, version=0.19.15
import pyarrow.parquet as pq

def csv_to_pandas(filepaths):
    df_result = pd.DataFrame()
    for path in filepaths:
        df_temp = pd.read_csv(path)
        # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
        df_result = pd.concat([df_result,df_temp])
    return df_result


#function for extract JSON file
def json_to_pandas(filepaths):
    df_result = pd.DataFrame()
    for path in filepaths:
        df_temp = pd.read_json(path)
        # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
        df_result = pd.concat([df_result,df_temp])
    return df_result

#function for extract parquet file
def parquet_to_pandas(filepaths):
    df_result = pd.DataFrame()
    for path in filepaths:
        df_temp = pq.read_table(path)
        df_temp = df_temp.to_pandas()
        # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
        df_result = pd.concat([df_result,df_temp])
    return df_result


#function for extract excel file
def excel_to_pandas(filepaths):
    df_result = pd.DataFrame()
    for path in filepaths:
        df_temp = pd.read_excel(path)
        # df_result = df_result.append(df_temp) #pakai pandas >= 1.4.0 akan error
        df_result = pd.concat([df_result,df_temp])
    return df_result

#function for extract AVRO file
def avro_to_pandas(filepaths):
    pl_result = pl.DataFrame() #menggunakan polars dataframe
    for path in filepaths:
        pl_temp = pl.read_avro(path)
        pl_result = pl.concat([pl_temp], how="vertical")

    df_result = pl_result.to_pandas()
    return df_result