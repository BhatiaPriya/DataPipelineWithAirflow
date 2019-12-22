from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """airflow operator to load fact table
    Arguments:
    table: fact table to be loaded from staging tables 
    redshift_conn_id: redshift connection id
    aws_conn_id: aws connection id
    insert_sql_query: sql query used to insert data into rows
    """
    
    ui_color = '#F98866'
    
    insert_query_fact = """
                        INSERT INTO {}
                        {};
                        """   
    
    @apply_defaults
    def __init__(self,
                 table = "",
                 redshift_conn_id = "",
                 aws_conn_id = "",
                 insert_sql_qry = "",
                 *args, **kwargs):
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.insert_sql_qry = insert_sql_qry

    def execute(self, context):
        """ loads fact table from staging table
        """
        self.log.info('LoadFactOperator not implemented yet')
        redshift = PostgresHook(postgres_conn_id = self.redshift_conn_id)
        
        self.log.info('loading fact table "{}" into redshift'.format(self.table))
        insert_query_fact = LoadFactOperator.insert_query_fact.format(self.table,
                                                                      self.insert_sql_qry,
                                                                      )
        redshift.run(insert_query_fact)