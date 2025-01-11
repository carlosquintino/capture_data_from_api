import great_expectations as ge
import pytz
from datetime import datetime
from utils import Utils



"""Modulo de qualidade que realiza testes pre definidos """


class Quality(Utils):

    def __init__(self,
                 job_name:str,
                 quality_params:dict,
                 trgt_tbl:str,
                 df,
                 stop_job:bool=False,
                 destination_on_failure:list=None,
                 destination_on_success:list=None,
                 spark=None):

        self.trgt_tbl = trgt_tbl
        timezone_sp = pytz.timezone("America/Sao_Paulo")
        self.now_sp = datetime.now(timezone_sp).strftime("%Y-%m-%d %H:%M:%S")
        self.subject = f"""{{status}} - {self.trgt_tbl} - {job_name}"""
        self.report = f"""<html>
                            <body>
                                <h3 style="color: {{color}};">
                                    <strong>[Data quality] - [{self.trgt_tbl}] - [{job_name}]</strong>
                                </h3>
                                <p><em>Execution Details:</em></p>
                                <ul>
                                    <li><strong>Table:</strong> {self.trgt_tbl}</li>
                                    <li><strong>Executed on:</strong> {self.now_sp}</li>
                                </ul>
                                <table border="1" cellpadding="5" cellspacing="0" style="border-collapse: collapse;">
                                    <tr>
                                        <th>Column</th>
                                        <th>Applied Test</th>
                                        <th>Status</th>
                                    </tr>
                        """
        self.df = df
        self.stop_job = stop_job
        self.destination_on_failure = destination_on_failure
        self.destination_on_success = destination_on_success
        self.quality_params = quality_params
        self.failed_expectation = False
        if spark:
            self.df_ge = self.convert_df_from_spark()
        else:
            self.df_ge = self.convert_df_from_pandas()
        super().__init__(job_name=job_name,table_name=trgt_tbl,source='quality')

    def generate_report(self,column=None,expectation=None):
        if column:
            type_validation = f"{column}"
        else:
            type_validation = f"Test on dataset"

        status = self.response['success']
        if status:
            report = f"""<tr>
                            <td>{type_validation}</td>
                            <td>{expectation}</td>
                            <td style="color: green;">Success</td>
                        </tr>"""
            
        else:
            report = f"""<tr>
                            <td>{type_validation}</td>
                            <td>{expectation}</td>
                            <td style="color: red;">Failed</td>
                        </tr>"""
            
            self.failed_expectation = True
        self.report += report

    def not_null(self,params):
        col_name = params['column']
        col_names = col_name.split(',')
        for col_name in col_names:
            self.response = self. \
                            df_ge. \
                                expect_column_values_to_not_be_null(
                                    col_name
                                    )
            self.generate_report(col_name,"Not Null values")
        
    
    def unique_vals(self,params):
        col_name = params['column']
        col_name = params['column']
        col_names = col_name.split(',')
        for col_name in col_names:
            self.response = self. \
                            df_ge. \
                                expect_column_values_to_be_unique(
                                    col_name
                                    )
            self.generate_report(col_name,"Unique values")
        

    def date_mask_equal(self,params):
        col_name = params['column']
        expected_format = params['date_mask']
        col_names = col_name.split(',')
        expected_formats = expected_format.split(',')
        for i in range(len(col_names)):
            col_name = col_names[i]
            expected_format = expected_formats[i]
            self.response = self. \
                            df_ge. \
                                expect_column_values_to_match_strftime_format(
                                    col_name,
                                    strftime_format=expected_format
                                    )
            self.generate_report(col_name,f"Date mask with mask {expected_format}")


    def value_lenght_btw(self,params):
        col_name = params['column']
        min_val = params['min']
        max_val = params['max']
        col_names = col_name.split(',')
        min_vals = min_val.split(',')
        max_vals = max_val.split(',')
        for i in range(len(col_names)):
            col_name = col_names[i]
            min_val = min_vals[i]
            max_val = max_vals[i]

            self.response = self. \
                                df_ge. \
                                expect_column_value_lengths_to_be_between(
                                    column=col_name,
                                    min_value=min_val,
                                    max_value=max_val)
            
            self.generate_report(col_name,f"Size of values between {min_val} and {max_val}")
        
        
    def df_count_btw(self,params):
        min_val = params['min']
        max_val = params['max']
        min_vals = min_val.split(',')
        max_vals = max_val.split(',')
        for i in range(len(min_vals)):
            min_val = min_vals[i]
            max_val = max_vals[i]

            self.response = self. \
                                df_ge. \
                                expect_table_row_count_to_be_between(
                                    min_val,
                                    max_val
                                )
            self.generate_report(expectation=f"Count between {min_val} and {max_val}")


    def value_match_regex(self,params):
        col_name = params['column']
        regex = params['regex']
        col_names = col_name.split(',')
        regexes = regex.split(',')
        for i in range(len(col_names)):
            col_name = col_names[i]
            regex = regexes[i]
            self.response = self.df_ge.\
                expect_column_values_to_match_regex(column=col_name,
                                                    regex=regex)
            self.generate_report(col_name,f"Column value {col_name} match with {regex}")
    
    
    def values_btw(self,params):
        col_name = params['column']
        min_val = params['min']
        max_val = params['max']
        col_names = col_name.split(',')
        min_vals = min_val.split(',')
        max_vals = max_val.split(',')
        for i in range(len(col_names)):
            col_name = col_names[i]
            min_val = min_vals[i]
            max_val = max_vals[i]
            self.response = self.df_ge.expect_column_values_to_be_between(
                column=col_name,
                min_value=min_val,
                max_value=max_val
            )
            self.generate_report(col_name,f"Values between {min_val} and {max_val}")
    
    
   
    def convert_df_from_pandas(self):
        return ge.from_pandas(self.df)
    

   
    def convert_df_from_spark(self):
        return ge.dataset.sparkdf_dataset.SparkDFDataset(self.df)

    
    #Monta checagem de relatorios
    def check_bdq(self,logger):
        
        checks = self.quality_params.keys()
        

        for check in checks:
            call_check = f"self.{check}({self.quality_params[check]})"
            print(f"Check {call_check}")
            eval(call_check)

        if self.destination_on_failure and self.failed_expectation:

            self.report = self.report.format(color='orange')
            self.subject = self.subject.format(status="failed")
            self.report += '\n</table>\n</body>\n</html>'

            print("Falha em teste")
            print(self.report)
            super().send_email(logger=logger,
                               recipient=self.destination_on_failure,
                               body=self.report,
                               subject=self.subject)

            if self.stop_job:
                raise Exception(f"One of data quality test return with failure")
        
        elif self.destination_on_success and not self.failed_expectation:

            self.report = self.report.format(color='green')
            self.subject = self.subject.format(status="Success")
            self.report += '\n</table>\n</body>\n</html>'

            super().send_email(logger=logger,
                               recipient=self.destination_on_success,
                               body=self.report,
                               subject=self.subject)
            

        print(self.report)
            