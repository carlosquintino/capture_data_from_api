import json
import pandas as pd
import os
from pathlib import Path
from datetime import datetime
import boto3

class Utils():

    def __init__(self,job_name,table_name,source) -> None:

        self.job_name = job_name
        self.table_name = table_name
        self.source = source
        self.base_path = Path(__file__).parent.parent 
        self.today = datetime.now().strftime('%Y%m%d')
        self.timestamp_now = datetime.now().strftime('%Y%m%d%H%M%S')

    def get_params(self,logger,file_path=None):
        try:
            logger.start_step('get_params')
            if not file_path:
                file_path = f'{self.base_path}/params/{self.source}/{self.table_name}.json'
            with open(file_path, 'r') as file:
                data = json.load(file)
            logger.end_step()
            return data,logger
            
        except Exception as e:
            logger.error(error_desc=str(e))
            raise Exception(f'Failed to try get the params {e}')
        
    def load_df_into_csv(self,df,logger,sep=';',index=False,):
        try:
            logger.start_step('load_csv')
            path_to_save = f'{self.base_path}/bronze/{self.source}/{self.table_name}/{self.today}/{self.today}.csv'

            directory = os.path.dirname(path_to_save)
            if not os.path.exists(directory):
                os.makedirs(directory)

            df.to_csv(path_to_save,sep=sep,index=index)
            logger.end_step()
        except Exception as e:
            logger.error(error_desc=str(e))
            raise Exception(f'Failed to try save the dataframe {e}')
    

    def send_email(self,logger,recipient,body,subject=""):
        
        file_path = f'{self.base_path}/params/aws/credentials.json'
        params,logger = self.get_params(logger=logger,file_path=file_path)
        access_key = params['access_key']
        secret_key = params['secret_key']
        ses = boto3.client('ses',
                           aws_access_key_id=access_key,
                           aws_secret_access_key=secret_key,
                           region_name='us-east-2')
        
        sender = "ambevcase@gmail.com" 
        
        response = ses.send_email(
        Source=sender,
        Destination={
            "ToAddresses": recipient,
        },
        Message={
            "Subject": {
                "Data": subject,
                "Charset": "UTF-8"
            },
            "Body": {
                "Html": {
                    "Data": body,
                    "Charset": "UTF-8"
                }
            }
        }
    )
        
    
    