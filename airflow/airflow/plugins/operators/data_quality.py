from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """airflow operator to check data quality in all the tables
    Arguments:
    tables: list of fact and dimension tables 
    redshift_conn_id: redshift connection id
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 tables = "",
                 redshift_conn_id = "",                 
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        """ checks data quality of all the tables
        """
        self.log.info('DataQualityOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for table in self.tables:
            self.log.info('data quality check on table "{}"'.format(table))
            
            records = redshift.get_records('SELECT COUNT(*) FROM "{}"'.format(table))
            self.log.info('total number of records are: {}'.format(records))
            self.log.info('zeroth record is: {} '.format(records[0]))
            
            self.log.info('length of records is: {} '.format(len(records)))
            self.log.info('length of zeroth record is: {} '.format(len(records[0])))
            
            num_records = records[0][0]
            self.log.info('count of rows are: {} '.format(num_records))
            
            if len(records) < 1 or len(records[0]) < 1:
                self.log.info('table "{}" doesnot contain any record'. format(table))
                raise ValueError('data quality check failed, table "{}" is not a valid table.format(table)')
            
            if num_records < 1:
                raise ValueError('data quality check failed, table "{}" contains 0 rows'.format(table))
            else:
                self.log.info('table "{}" contains records: '.format(table))
            self.log.info('data quality check done!')