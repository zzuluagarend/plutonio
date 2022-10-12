import pandas as pd

import sql_queries 
from analystcommunity import read_connection_data_warehouse
from analystcommunity import write_connection_data_warehouse
import time

from sql_queries.queries_experiment import exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8

def sql_query_clean_table(datasets_id, experiment):
    
    query= f"""  
                 DELETE
                 FROM lnd_ops_dev.dp_plutonio
                 WHERE dp_input_datasets_id = {datasets_id}
            """
    write_connection_data_warehouse.runQuery('ops', query)

def clean_daily_data(datasets_id, date, experiment):
    
    query= f"""
                DELETE 
                FROM lnd_ops_dev.dp_plutonio
                WHERE dp_input_datasets_id = {datasets_id}
                    AND date = DATE('{date}')
            """
    write_connection_data_warehouse.runQuery('ops', query)

def write_plutonio(demand_plutonio, batch_size = 3000):
    
    BATCH_SIZE = batch_size
    df_size = len(demand_plutonio)
    for i in range(0, (df_size // BATCH_SIZE) + 1):
        low = i * BATCH_SIZE
        high = min((i + 1) * BATCH_SIZE, df_size)
        print('Storing dataframe from row index %d to row index %d' % (low, high - 1))
        df_batch = demand_plutonio.iloc[low:high, :]
   
        write_connection_data_warehouse.to_sql('ops', df_batch, 'dp_plutonio')

    
def run_demand_plutonio(country, input_datasets_id, initial_load = False):
    
    #demand_plutonio = pd.DataFrame()
    exps = [exp1,exp2,exp3,exp4,exp5,exp6,exp7,exp8]
    
    if initial_load:
        restriction = ""
        sql_query_clean_table(input_datasets_id)
    else:
        restriction = f"""DATE(date) = '{pd.to_datetime('today').date()}'"""
        clean_daily_data(input_datasets_id, pd.to_datetime('today').date())
    
    for exp in exps:
        query = exp(country, input_datasets_id, restriction)
        demand_plutonio = read_connection_data_warehouse.run_read_prod_query(query)
        write_plutonio(demand_plutonio)
    
    #return demand_plutonio
