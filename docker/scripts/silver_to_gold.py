
from utils import Utils
from logs import Logs
import argparse
from pathlib import Path
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, IntegerType, FloatType, DoubleType, LongType, BooleanType, DateType, TimestampType


job_name = 'silver_to_gold'

parser = argparse.ArgumentParser(description="Capture from api")

parser.add_argument("--table_name", type=str, help="Name of the table to be put in gold layer", required=True)
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

def create_table(schema:dict,target_schema:str,target_table:str,partition_column:str=None) -> str:
    columns = [f"{col} {dtype}" for col, dtype in schema.items() if col != partition_column]
    columns_sql = ",\n    ".join(columns)
    sql = f"""
    CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (
        {columns_sql}
    )"""

    if partition_column:
        sql += f"""PARTITIONED BY ({partition_column} {schema[partition_column]} )"""
    
    sql += F"""STORED AS PARQUET
                LOCATION 'hdfs://namenode:8020/user/case-datalake/gold/{target_schema}/{target_table}'"""
    
    return sql

def read_sql(table,logger):
    try:
        logger.start_step('read_sql')
        path = f'{base_path}/query/gold/{table}.sql'
        with open(path,'r') as file:
            content = file.read()
        logger.end_step()
        return content,logger
    except Exception as e:
        logger.error(error_desc=str(e))
        raise Exception('File not found')

def main(logger):
    params,logger = utils_instance.get_params(logger=logger)

    target_schema = params['target_schema']
    target_table = params['target_table']
    partition_column = params.get('partition_column',None)
    mode = params['mode']

    sql_query,logger = read_sql(target_table,logger)

    try:
        logger.start_step('run_query')
        df = spark.sql(sql_query)
        logger.end_step()
    except Exception as e:
        logger.error(error_desc=str(e))
        raise Exception('Error to try run query')
    
    if not spark.catalog.databaseExists(target_schema):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {target_schema}")

    type_mapping = {
    StringType: "STRING",
    IntegerType: "INT",
    FloatType: "FLOAT",
    DoubleType: "DOUBLE",
    LongType: "BIGINT",
    BooleanType: "BOOLEAN",
    DateType: "DATE",
    TimestampType: "TIMESTAMP"
    }

    schema = {
    field.name: type_mapping[type(field.dataType)]
    for field in df.schema.fields
    }

    if not spark.catalog.tableExists(f'{target_schema}.{target_table}'):
        create_table_sql = create_table(schema=schema,target_schema=target_schema,target_table=target_table,partition_column=partition_column)
        spark.sql(create_table_sql)

    
    df.write.format("parquet").mode(mode).insertInto(f'{target_schema}.{target_table}')


if __name__ == '__main__':
    main(logger=logger)
    logger.write_log()
    

