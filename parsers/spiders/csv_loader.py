import pandas as pd
from database.connector import Connector

def load_csv_interest_rate(
    file_path):
    file = pd.read_csv(file_path)
    start_date = file['START_DATE']
    end_date   = file['END_DATE']
    value      = file['INTEREST_RATE']
    idx        = 0
    connector = Connector()
    conn = connector.connect()
    while idx < len(start_date):
        conn.execute(f'''
            INSERT INTO FACT_INDICATORS(
                INDICATORKEY, INDICATORID, 
                FROMDATE, TODATE, INDICATORVALUE)
            VALUES(
                (SELECT INDICATORKEY FROM DIM_INDICATORS 
                        WHERE INDICATORNAME = 'Interest Rate' AND ISCURRENT = 'Yes'),
                (SELECT INDICATORID FROM DIM_INDICATORS 
                        WHERE INDICATORNAME = 'Interest Rate' AND ISCURRENT = 'Yes'),
                TO_DATE('{start_date[idx]}', 'YYYY-MM-DD'), 
                TO_DATE('{end_date[idx]}', 'YYYY-MM-DD'), 
                CAST('{value[idx]}' AS NUMERIC(14, 2)))
        ''')
        idx+=1

def load_csv_inflation_rate(
    file_path):
    file = pd.read_csv(file_path)
    start_date = file['DATE']
    value      = file['INFLATION']
    idx        = 0
    connector = Connector()
    conn = connector.connect()
    while idx < len(start_date):
        conn.execute(f'''
            INSERT INTO FACT_INDICATORS(
                INDICATORKEY, INDICATORID, 
                FROMDATE, TODATE, INDICATORVALUE)
            VALUES(
                (SELECT INDICATORKEY FROM DIM_INDICATORS 
                        WHERE INDICATORNAME = 'Inflation Rate' AND ISCURRENT = 'Yes'),
                (SELECT INDICATORID FROM DIM_INDICATORS 
                        WHERE INDICATORNAME = 'Inflation Rate' AND ISCURRENT = 'Yes'),
                TO_DATE('{start_date[idx]}', 'YYYY-MM-DD'), 
                ADD_MONTHS(TO_DATE('{start_date[idx]}', 'YYYY-MM-DD'), 1), 
                CAST('{value[idx]}' AS NUMERIC(14, 2)))
        ''')
        idx+=1

load_csv_interest_rate() # file_path='... .csv'
load_csv_inflation_rate() # file_path='... .csv'