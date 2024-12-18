import argparse
import pandas as pd
import requests
import os
import json
from logs import Logs
from utils import Utils


"""This job is designed to perform an API capture. It expects the following inputs:
--table_name: The name of the capture table
--source: The name of the data provider source
The job retrieves other necessary parameters from params/{source}/table_name.json"""

job_name='capture_api'

parser = argparse.ArgumentParser(description="Capture from api")

parser.add_argument("--table_name", type=str, help="Name of the table to be captured", required=True)
parser.add_argument("--source", type=str, help="Name of the data source", required=True)
parser.add_argument("--email_on_failure", type=str, help="Emails to send on failure", required=False,  default=None)
args = parser.parse_args()

table_name = args.table_name
source = args.source
email_on_failure = args.email_on_failure

logger = Logs(job_name=job_name, table_name=table_name, source=source, email_on_failure=email_on_failure)

utils_istance = Utils(job_name=job_name, table_name=table_name, source=source)


def connect_with_api(url) -> json:
    try:
        logger.start_step('connect_api')
        response = requests.get(url)
        status_code = response.status_code 
        if response.status_code != 200:
            logger.error(f'Api return code {status_code}')
            raise
        logger.end_step()
        return response.json()
    except Exception as e:
        logger.error(error_desc=str(e))
        raise

def main(logger):

    params,logger = utils_istance.get_params(logger=logger)
    url = params.get('url')
    data = connect_with_api(url=url)
    df = pd.DataFrame(data)
    utils_istance.load_df_into_csv(df=df,logger=logger)

if __name__ == '__main__':
    main(logger=logger)
    logger.write_log()