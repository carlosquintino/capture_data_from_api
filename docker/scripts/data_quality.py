import argparse
from pyspark.sql import SparkSession
from quality import Quality
from utils import Utils
from logs import Logs
from pathlib import Path


spark = SparkSession.builder \
    .appName("SparkHiveIntegration") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
    .config("spark.sql.parquet.compression.codec", "uncompressed") \
    .config("spark.sql.hive.metastore.logging.level", "DEBUG") \
    .enableHiveSupport() \
    .getOrCreate()


job_name = 'data_quality'

parser = argparse.ArgumentParser(description="Capture from api")



parser.add_argument("--table_name", type=str, help="Name of the table to be captured", required=True)
parser.add_argument("--email_on_failure", type=str, help="Emails to send on failure", required=False,  default=None)
parser.add_argument("--email_on_success", type=str, help="Emails to send on success", required=False,  default=None)

args = parser.parse_args()

table_name = args.table_name
email_on_failure = args.email_on_failure
email_on_success = args.email_on_success

if email_on_failure:
    email_on_failure = email_on_failure.split(',')

if email_on_success:
    email_on_success = email_on_success.split(',')

utils_instance = Utils(job_name=job_name,table_name=table_name,source='quality')
logger = Logs(job_name=job_name,table_name=table_name,source='quality',email_on_failure=email_on_failure)

base_path = Path(__file__).parent.parent

def read_sql(table,logger):
    try:
        logger.start_step('read_sql')
        path = f'{base_path}/query/quality/{table}.sql'
        with open(path,'r') as file:
            content = file.read()
        logger.end_step()
        return content,logger
    except Exception as e:
        logger.error(error_desc=str(e))
        raise Exception('File not found')


def main(logger):

    params,logger = utils_instance.get_params(logger=logger)
    quality_params = params['quality_params']
    sql_query,logger = read_sql(table=table_name,logger=logger)

    df = spark.sql(sql_query)

    quality_instance = Quality(job_name=job_name,
                               quality_params=quality_params,
                               trgt_tbl=table_name,
                               df=df,
                               destination_on_failure=email_on_failure,
                               destination_on_success=email_on_success,
                               spark=spark)
    
    quality_instance.check_bdq(logger=logger)

if __name__ == '__main__':
    main(logger=logger)
    logger.write_log()