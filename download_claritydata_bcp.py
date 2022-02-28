import bcp
import datetime
from config import server_info, server_info_dev, database_testing, first_query, database_upload
import warnings
import logging
import time

import multiprocessing
# print(multiprocessing.cpu_count())

from pathlib import Path

# Disable the warnings
warnings.filterwarnings('ignore')

path_raw_files = '.\\raw_files\\'
# path_raw_files=r'C:\Users\sartoria1\Desktop\prova\raw_files'
# print()
today = datetime.datetime.now().strftime('%Y-%m-%d')
filename = f'dump_{today}'
# filename = f'dump_{time}'
# connect to the database Clarity
def download_clarity(path_raw_files,filename, database):

    conn = bcp.Connection(host= server_info, driver='mssql')
    my_bcp = bcp.BCP(conn)

    path = Path(f'{path_raw_files}{filename}.csv')
    file = bcp.DataFile(file_path= path, delimiter='|')
    my_bcp.dump(query= f'''SELECT LOC_NAME 
                ,FACILITY_ID
                ,DEPARTMENT_NAME
                ,DEPARTMENT_ID
                ,HOSPITAL_SERVICE
                ,PAT_ID 
                ,PAT_NAME
                , MRN
                ,CONTACT_DATE
                ,BIRTH_DATE
                ,ORDER_TIME
                ,ORD_RESULT_TIME
                , ORDER_PROC_ID
                ,SPECIMEN_TYPE
                , PROC_NAME
                , COVID19_PROC_TYPE
                , AGE
                , AGE_GROUP
                ,PATIENT_SEX 
                ,PATIENT_RACE
                ,PATIENT_LANGUAGE 
                ,RESULT
                ,ZIP_CODE
                ,HOSP_ADMSN_TIME
                ,INP_ADM_DATE
                ,HOSP_DISCH_TIME
                ,Death_Date
                ,Disch_Disposition
                ,Site_Type
                ,PRIMARY_PAYER_GROUP
                ,admit_dep_name
                ,Borough
                ,Neighborhood
        FROM [CovResponse].[Tableau].[V_COVID19_PCR_Testing_All_Care_Settings]''', output_file=file)


#entrypoint
if __name__ == '__main__':

    start = time.time()
    download_clarity(path_raw_files=path_raw_files,filename=filename, database=database_testing)
    stop = time.time()
    print(stop - start)

