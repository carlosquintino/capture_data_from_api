from datetime import datetime
import pandas as pd
import boto3
from utils import Utils
import pytz
from pyspark.sql import SparkSession
from pyspark.sql.functions import *




"""The logs code is designed to log each step of the execution, recording its runtime to help identify any steps that take longer than expected. 
   Additionally, it allows for the inclusion of more useful columns using the info function, such as the number of rows in a capture, 
   the file extension of the received data, etc. The code saves the generated log in a database under the schema logs.job_name"""

class Logs(Utils):
    
    def __init__(self,job_name,table_name,source,email_on_failure=None) -> None:
        now = datetime.now()
        self.today = now.strftime('%y-%M-%d')
        self.job_name = job_name
        timezone_sp = pytz.timezone("America/Sao_Paulo")
        self.now_sp = datetime.now(timezone_sp).strftime("%Y-%m-%d %H:%M:%S")
        self.file_name = datetime.now(timezone_sp).strftime("%Y_%m_%d_%H_%M_%S")
        
        self.email_on_failure = email_on_failure

        self.log = {'start_exec':now,'job_name':job_name,'status':'success','table_name':table_name,'source':source,'info':'','error':'','error_desc':''}
        super().__init__(job_name=job_name,table_name='credentials',source='aws')

        self.spark = SparkSession.builder \
            .appName("SparkHiveIntegration") \
            .config("spark.sql.catalogImplementation", "hive") \
            .config("hive.metastore.uris", "thrift://hive-metastore:9083") \
            .config("spark.sql.parquet.compression.codec", "uncompressed") \
            .config("spark.sql.hive.metastore.logging.level", "DEBUG") \
            .enableHiveSupport() \
            .getOrCreate()
    

    def start_step(self,step_name):
        self.step_name = step_name
        self.start_step_time = datetime.now()
    

    def end_step(self):
        end_step = datetime.now()
        duration = (end_step - self.start_step_time).total_seconds()
        self.log['info'] += f'{self.step_name}:{duration} \n'
    

    def info(self,col,msg):
        self.log[col] = msg


    def write_log(self):
        self.log['end_exec'] = datetime.now()
        self.log['date_ref'] = self.today
        print(self.log)
        df = self.spark.createDataFrame([self.log])
        print(df)

        if not self.spark.catalog.databaseExists('logs'):
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS logs")

        df = df.select(
                                col("start_exec").cast("timestamp"),
                                col("job_name"),
                                col("status"),
                                col("table_name"),
                                col("source"),
                                col("info"),
                                col("error"),
                                col("error_desc"),
                                col("date_ref"),
                                col("end_exec").cast("timestamp")
                            )
        if not self.spark.catalog.tableExists(f'logs.{self.job_name}'):
            create_table_sql = f"""CREATE TABLE IF NOT EXISTS logs.{self.job_name} (
                                    start_exec TIMESTAMP,
                                    job_name STRING,
                                    status STRING,
                                    table_name STRING,
                                    source STRING,
                                    info STRING,
                                    error STRING,
                                    error_desc STRING,
                                    date_ref STRING,
                                    end_exec  TIMESTAMP
                                )
                                STORED AS PARQUET
                                LOCATION 'hdfs://namenode:8020/user/case-datalake/logs/{self.job_name}/';
                                """
            self.spark.sql(create_table_sql)
        df.write.format("parquet").mode("append").insertInto(f'logs.{self.job_name}')

    def error(self,error_desc):
        self.log['status'] = 'failed'
        self.log['error'] = f'Error while trying to execute the step {self.step_name}'
        self.log['error_desc'] = error_desc
        
        if self.email_on_failure:
            if type(self.email_on_failure) == str:
                self.email_on_failure = self.email_on_failure.split(',')

            body = f"""<html>
                            <body>
                                <h3 style="color: red;">
                                    <strong>[Data Quality] - [{self.table_name}] - [{self.job_name}]</strong>
                                </h3>
                                <p><em>Execution Details:</em></p>
                                <ul>
                                    <li><strong>Table:</strong> {self.table_name}</li>
                                    <li><strong>Executed on:</strong> {self.now_sp}</li>
                                    <li><strong>Error:</strong> Error while trying to execute the step {self.step_name}</li>
                                </ul>
                            </body>
                        </html>"""
            super().send_email(logger=self,recipient=self.email_on_failure,body=body,subject=f"Error in table {self.table_name}")
        
        
        self.write_log()
       