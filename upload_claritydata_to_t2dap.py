import chunk
import time
import pandas as pd
import datetime
import warnings

import pyodbc

from config import server_info, server_info_dev, database_testing, first_query, database_upload, load_table

import logging

# Disable the warnings
warnings.filterwarnings('ignore')


path_raw_files = '.\\raw_files\\'
# path_raw_files=r'C:\Users\sartoria1\Desktop\prova\raw_files'
# print()
today = datetime.datetime.now().strftime('%Y-%m-%d')



# connect to the database Clarity


def create_server_connection(server, database):
    conn = None
    try:
        conn = pyodbc.connect(
                Trusted_Connection='Yes',
                Driver='{ODBC Driver 17 for SQL Server}',
                Server= server,
                Database= database
            )
        print('SQL Server Database connection successful')

    except pyodbc.Error as e:
        print(f'Error: {e}')

    return conn


''' Load extracted data tp table the the specific view in CERT '''

def load_data(cursor, df, table):

    # Insert DataFrame to Table
    insert_to_tmp_tbl=f"INSERT INTO {table} VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"
    
    cursor.fast_executemany = True
    cursor.executemany(insert_to_tmp_tbl, df.values.tolist())
    print(f'{len(df)} rows inserted to the {table} table')

    cursor.commit()


    # for row in df.itertuples():
    #     cursor.execute('''
    #                 INSERT INTO products (product_id, product_name, price)
    #                 VALUES (?,?,?)
    #                 ''',
    #                 row.product_id, 
    #                 row.product_name,
    #                 row.price
    #                 )
    # conn.commit()



#entrypoint
if __name__ == '__main__':

    
    # connection = create_server_connection(server=server_info, database=database_testing)
    # start = time.time()

    # new_df = read_query(connection, first_query)
    # new_df.to_csv(f'{path_raw_files}{filename}.csv', index=False)
    
    columns = ['loc_name', 'facility_id', 'department_name', 'department_id','hospital_service', 'pat_id', 'pat_name', 'mrn','contact_date', 
                'birth_date','order_time', 'ord_result_time', 'order_procedure_id','specimen_type', 'procedure_name', 'covid19_proc_type', 'age', 'age_group',
                'patient_sex', 'patient_race', 'patient_language', 'result',
                'zipcode','hosp_adm_time', 'inpatient_adm_date', 'hosp_disch_time',
                'death_date', 'discharge_disposition',  'site_type',
                'primary_payer_group', 'admit_dep_name', 
                'borough', 'neighborhood']

    
    connection_to_load_tdap = create_server_connection(server=server_info_dev, database=database_upload)
    cursor=connection_to_load_tdap.cursor()
    # start = time.time()
    filename = f'dump_{today}'
    path_filename = f'{path_raw_files}{filename}.csv'
    chunck = pd.read_csv(path_filename, delimiter='|' , dtype={"loc_name": str, "admit_dep_name":str ,"department_id":object ,"hospital_service":str}, names = columns, skiprows = 1000000, parse_dates=['birth_date','contact_date', 'ord_result_time','order_time','hosp_disch_time','inpatient_adm_date','hosp_adm_time','death_date'], chunksize=100000)
    # df= pd.read_csv(path_filename, delimiter='|' , dtype={"loc_name": str, "admit_dep_name":str ,"department_id":object ,"hospital_service":str}, names = columns, nrows=50, parse_dates=['birth_date','contact_date', 'ord_result_time','order_time','hosp_disch_time','inpatient_adm_date','hosp_adm_time','death_date'])
    # ,  dtype={"salary": int}

    # print(len(df.loc[9]['primary_payer_group']))
    
    for ii, df in enumerate(chunck):
        # print(df['order_procedure_id'])
        start = time.time()
        

        df = df.astype(object).where(pd.notnull(df), None)
        df['crtd_dt'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        # print(df.info())
        # df = df.fillna("NULL")
        # print(df.loc[0])
        # print(df['death_date'])
        # print(df['pat_name'])
        print(ii)
        # print(df)
        load_data(cursor,df,table=load_table)
    
        stop = time.time()
        print('Time elapsed: ', stop - start)
    #     # print(df['mrn'].str.len().max())
    #     # break
    #     # if(ii == 5):
    #     #     break
    # cursor.close()
    # connection_to_load_tdap.close()



    # print(df)
        # df[['birth_date','contact_date', 'ord_result_time','order_time','hosp_disch_time','inpatient_adm_date','hosp_adm_time','death_date']])
        # df['birth_date'] = df[df['birth_date'].where(df['birth_date'].isnull(),'1960-08-15')
        # df['contact_date'] = df['contact_date'].where(df['contact_date'].isnull(),'1960-08-15')
        # df['ord_result_time'] = df['ord_result_time'].where(df['ord_result_time'].isnull(), '1960-08-15')
        # df['order_time'] = df['order_time'].where(df['order_time'].isnull(), '1960-08-15')
        # df['hosp_disch_time'] = df['hosp_disch_time'].astype(object).where(df['hosp_disch_time'].isnull(), '1960-08-15')
        # df['inpatient_adm_date'] = df['inpatient_adm_date'].astype(object).where(df['inpatient_adm_date'].isnull(), '1960-08-15')
        # df['hosp_adm_time'] = df['hosp_adm_time'].astype(object).where(df['hosp_adm_time'].isnull(), '1960-08-15')

        # df.loc[df['birth_date'].isnull(), 'birth_date'] = datetime.datetime.now()
        # df.loc[df['contact_date'].isnull(), 'contact_date'] = datetime.datetime.now()
        # df.loc[df['ord_result_time'].isnull(), 'ord_result_time'] = datetime.datetime.now()
        # df.loc[df['order_time'].isnull(), 'order_time'] = datetime.datetime.now()

        # df.loc[df['hosp_disch_time'].isnull(), 'hosp_disch_time'] = datetime.datetime.now()
        # df.loc[df['inpatient_adm_date'].isnull(), 'inpatient_adm_date'] = datetime.datetime.now()
        # df.loc[df['hosp_adm_time'].isnull(), 'hosp_adm_time'] = datetime.datetime.now()
        # df.loc[df['death_date'].isnull(), 'death_date'] = datetime.datetime.now()