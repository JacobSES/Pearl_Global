import time
import datetime
import streamlit as st
import apscheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor
from Report_TDU import TDU
from Report_Email import EMAIL
from Report_Dashboard import DASHBOARD

def tdu4_task(TDU4):
    TDU4.read_tdu_data()
    TDU4.calculate_production()
    TDU4.calculate_tdu_availability()
    TDU4.calculate_tdu_throughput()
    TDU4.export_to_csv()
    # TDU4.update_performance_table()

def main():
    TDU_LIST = []
    for i in range(4):
        databaseName = f'TDU0{i+1}'
        TDU_LIST.append(TDU(i+1, databaseName))

    tdu4_task(TDU_LIST[3])
    dash = DASHBOARD(TDU_LIST)
    
if __name__=="__main__":
    main()