
from utils import Utils
from logs import Logs
import argparse
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *


job_name = 'bronze_to_silver'

parser = argparse.ArgumentParser(description="Capture from api")

parser.add_argument("--table_name", type=str, help="Name of the table to be captured", required=True)
parser.add_argument("--source", type=str, help="Name of the data source", required=True)
parser.add_argument("--email_on_failure", type=str, help="Emails to send on failure", required=False,  default=None)
args = parser.parse_args()

table_name = args.table_name
source = args.source
email_on_failure = args.email_on_failure

utils_instance = Utils(job_name=job_name,table_name=table_name,source=source)
logger = Logs(job_name=job_name,table_name=table_name,source=source,email_on_failure=email_on_failure)

base_path = Path(__file__).parent.parent
today = datetime.now().strftime('%Y%m%d') 

spark = SparkSession.builder \
    .appName("SparkHiveIntegration") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.parquet.compression.codec", "uncompressed") \
    .config("spark.sql.hive.metastore.logging.level", "DEBUG") \
    .enableHiveSupport() \
    .getOrCreate()

def read_csv_file(path,logger,params):
    try:
        logger.start_step('read_csv_file')
        df = spark.read.csv(path,sep=params.get('sep',';'),header=params.get('header',True))
        logger.end_step()
        return df
    except Exception as e:
        logger.error(error_desc=str(e))
        raise Exception(f'Failed to try read the csv file {e}')
    

def cast_dataframe(df,logger,schema,partition_column):

    try:
        logger.start_step("cast_df")
        #cast on dataframe
        df = df.select([col(c).cast(t) for c, t in schema.items() if c in df.columns])

        #Put the partition_colum in the last position
        if partition_column and partition_column in df.columns:
            other_columns = [c for c in df.columns if c != partition_column]
            df = df.select(*other_columns, partition_column)

        return df
    except Exception as e:
        logger.error(error_desc=str(e))


def create_table(schema:dict,target_schema:str,target_table:str,partition_column:str=None) -> str:
    columns = [f"{col} {dtype}" for col, dtype in schema.items() if col != partition_column]
    columns_sql = ",\n    ".join(columns)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (
        {columns_sql}
    )"""

    if partition_column:
        sql += f"""PARTITIONED BY ({partition_column} {schema[partition_column]} )"""
    
    sql += f"""STORED AS PARQUET
                LOCATION 'hdfs://namenode:8020/user/case-datalake/silver/{target_schema}/{target_table}'"""
    
    return sql


def main(logger):

    params,logger = utils_instance.get_params(logger=logger)
    ext = params.get('ext','csv')
    schema = params.get('schema',None)
    path = f'{base_path}/bronze/{source}/{table_name}/{today}'
    target_schema = params['target_schema']
    target_table = params['target_table']
    partition_column = params.get('partition_column',None)
    mode = params['mode']

    
    if ext == 'csv':
        df = read_csv_file(path=path,params=params,logger=logger)
    else:
        #New kind of files
        pass

    if schema:
        df = cast_dataframe(df,logger=logger,schema=schema,partition_column=partition_column)
    
    if not spark.catalog.databaseExists(target_schema):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_schema}")
    
    if not spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        create_table_sql = create_table(schema=schema,target_schema=target_schema,target_table=target_table,partition_column=partition_column)
        spark.sql(create_table_sql)
    
    df.write.format("parquet").mode(mode).insertInto(f'{target_schema}.{target_table}')
    

    
    
if __name__ == '__main__':
    main(logger=logger)
    logger.write_log()
