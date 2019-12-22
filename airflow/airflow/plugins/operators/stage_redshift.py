from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    """airflow operator to load data from s3 to staging tables 
    Arguments:
    table: staging table to load data into
    json_paths: json paths of staging data files   
    file_type: file types of data files from s3 (json or csv)
    redshift_conn_id: redshift connection id
    aws_conn_id: aws connection id
    s3_bucket: s3 bucket
    s3_key: s3 key
    delimiter: delimiter used in staging data files
    """
    
    ui_color = '#358140'

    copy_sql_json = """
                    COPY {}
                    FROM '{}'
                    json '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    """
    
    copy_sql_csv =  """
                    COPY {}
                    FROM '{}'
                    ACCESS_KEY_ID '{}'
                    SECRET_ACCESS_KEY '{}'
                    IGNOREHEADER {}
                    DELIMITER '{}'
                    """
    
    @apply_defaults
    def __init__(self,
                 table = "",
                 json_path = "",
                 file_type = "",
                 redshift_conn_id = "",
                 aws_conn_id = "",
                 s3_bucket = "",
                 s3_key = "",
                 ignore_header = 1,
                 delimiter = ",",                 
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.json_path = json_path
        self.file_type = file_type
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.ignore_header = ignore_header
        self.delimiter = delimiter


    def execute(self, context):
        """ copies data from s3 into staging table. Format of the data could be either json or csv.
        """
        self.log.info('StageToRedshiftOperator not implemented yet')

        aws_hook = AwsHook(aws_conn_id = self.aws_conn_id)
        credentials = aws_hook.get_credentials()

        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)

        self.log.info("Clearing data from destination Redshift table")
#         redshift.run("DELETE FROM {}".format(self.table))

        self.log.info('creating s3 path')
        s3_path = "s3://{}/{}".format(self.s3_bucket, self.s3_key.format(**context)) # need to check it later
        self.log.info(s3_path)

        if self.file_type == "json":           
            copy_query = StageToRedshiftOperator.copy_sql_json.format(self.table,
                                                                      s3_path,
                                                                      self.json_path,
                                                                      credentials.access_key,
                                                                      credentials.secret_key,
                                                                      )

            redshift.run(copy_query)
            self.log.info('successfully ran')
        elif self.file_type == 'csv':
            copy_query = StageToRedshiftOperator.copy_sql_csv.format(self.table,
                                                                     s3_path,
                                                                     credentials.access_key,
                                                                     credentials.secret_key,
                                                                     self.ignore_header,
                                                                     self.delimiter,
                                                                     )
        else:
            self.log.info('not a valid format, pls check it!')