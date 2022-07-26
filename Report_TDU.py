import pandas as pd
import numpy as np
import datetime
import sqlalchemy as sal
import streamlit as st
import pyodbc
from sqlalchemy.sql import text
from sqlalchemy.sql import insert

class TDU:
    
    def __init__(self, unit_Number, database):

        self.tdu_unit = unit_Number
        self.database = database
        self.user = 'Pearl_Global'
        self.password = 'Pearl737!!'
        self.server = 'SES_UNIT_01\SQLEXPRESS'
        self.dataframe = {}
        self.table_array = ["PRODUCTION_REPORT", "DOWNTIME_REPORT"]
        self.tdu_availability = 0
        self.tdu_tonnes_processed = 0
        self.tdu_throughput = 0

    def read_tdu_data(self): 
        for tableName in self.table_array:
            if tableName == self.table_array[0]: #read production report from SQL
                # query = f"SELECT * FROM {database}.dbo.{tableName} WHERE Timestamp BETWEEN '{yesterday} 06:00:00' AND '{reporting_date} 06:00:00'"
                query = f"SELECT * FROM {self.database}.dbo.{tableName}" ##This is just for testing query
                self.dataframe[self.table_array[0]] = self.read_from_sql(query)

            elif tableName == self.table_array[1]: #read downtime report from SQL
                # query = f"SELECT * FROM {database}.dbo.{tableName} WHERE Start_Time BETWEEN '{yesterday} 06:00:00' AND '{reporting_date} 06:00:00'"
                query = f"SELECT * FROM {self.database}.dbo.{tableName}" ##This is just for testing query
                self.dataframe[self.table_array[1]] = self.read_from_sql(query)
                self.dataframe[self.table_array[1]]["Downtime_Duration"] = self.dataframe[self.table_array[1]]["End_Time"] - self.dataframe[self.table_array[1]]["Start_Time"]

    # @st.cache(allow_output_mutation=True)
    def read_from_sql(self, query):

        # URL = f'mssql+pyodbc://{self.user}:{self.password}@SES_UNIT_01\SQLEXPRESS/{self.database}?driver=ODBC+Driver+17+for+SQL+Server'
        URL = f'mssql+pyodbc://{self.user}:{self.password}@192.168.250.49:1433/{self.database}?driver=ODBC+Driver+17+for+SQL+Server'
        engine = sal.create_engine(URL)
        sql_query = pd.read_sql_query(query, engine.connect())
        return pd.DataFrame(sql_query)

    # @st.cache(allow_output_mutation=True)
    def write_to_sql(self, insert_row, table_name):
        user = 'Pearl_Global'
        password = 'Pearl737!!'
        insert_row_df = pd.DataFrame(data = insert_row)
        URL = f'mssql+pyodbc://{user}:{password}@localhost:1433/{self.database}?driver=SQL+Server'
        engine = sal.create_engine(URL) 
        insert_row_df.to_sql(table_name, con=engine, if_exists='append', index = False)
        engine.execute(f"SELECT * FROM {table_name}").fetchall()

    def export_to_csv(self):
        for tableName in self.table_array:
            if tableName == self.table_array[0]: #read production report from SQL
                rawdata_csv = (str)(datetime.date.today()) + f"_TDU{self.tdu_unit}_"+ self.table_array[0] + ".csv"
                file_address = f"C:/Users/61499/OneDrive - SES/Pearl Global/Python/CSV/production/{rawdata_csv}"    
                self.dataframe[self.table_array[0]].to_csv(file_address, index = False) # file Location  

            elif tableName == self.table_array[1]: #read downtime report from SQL
                rawdata_csv = (str)(datetime.date.today()) + f"_TDU{self.tdu_unit}_"+ self.table_array[1] + ".csv"
                file_address = f"C:/Users/61499/OneDrive - SES/Pearl Global/Python/CSV/operation/{rawdata_csv}"
                self.dataframe[self.table_array[1]].to_csv(file_address, index = False) # file Location

    def calculate_production(self):
        self.dataframe[self.table_array[0]]['Throughput_kg'] = "0"
        self.dataframe[self.table_array[0]]['Throughput_kg'] = pd.to_numeric(self.dataframe[self.table_array[0]]['Throughput_kg'])
        self.dataframe[self.table_array[0]].dropna(how='all')

        for index, row in self.dataframe[self.table_array[0]].iterrows():
            if index == 0:
                previous_temp = row['Weight_feed_belt_kg']
            else:
                usage = row['Weight_feed_belt_kg'] - previous_temp
                previous_temp = row['Weight_feed_belt_kg']
                if usage < 0:
                    self.dataframe[self.table_array[0]].loc[index,'Throughput_kg'] = usage
        
        self.dataframe[self.table_array[0]]['Throughput_kg'] *= -1
        self.daily_total = self.dataframe[self.table_array[0]]['Throughput_kg'].sum()
        self.tdu_tonnes_processed = self.daily_total/1000
        # half_number = (int)((len(self.dataframe[self.table_array[0]]) - 1)/2)
        # self.day_df = self.dataframe[self.table_array[0]].iloc[:(half_number-1),:]
        # self.night_df = self.dataframe[self.table_array[0]].iloc[half_number:,:]
        # self.day_total = self.day_df['Throughput_kg'].sum()
        # self.night_total = self.night_df['Throughput_kg'].sum()

    def calculate_tdu_availability(self):
        downtime_total = 0
        for i in self.dataframe[self.table_array[0]]["Throughput_kg"]:
            if i == 0:
                downtime_total += 1
        self.dataframe[self.table_array[1]]["Downtime_Duration_MIN"] = self.dataframe[self.table_array[1]]["Downtime_Duration"].dt.total_seconds().divide(60)
        downtime_total += self.dataframe[self.table_array[1]]["Downtime_Duration_MIN"].sum()
        self.tdu_availability = round((downtime_total/(24*60)) * 100)

    def calculate_tdu_throughput(self):
        self.tdu_throughput = self.tdu_tonnes_processed / (self.tdu_availability * 24)

    def update_performance_table(self):
        append_row = {"DATE": [datetime.date.today()], "Availability": [self.tdu_availability], "Throughput": [self.tdu_throughput], 
                            "Tonnes_Processed": [self.tdu_tonnes_processed]}
        self.write_to_sql(append_row, "PERFORMANCE_REPORT")
        append_row.clear()
  
    def get_tdu_availability(self):
        return self.tdu_availability
    
    def get_tdu_tonnes_processed(self):
        return self.tdu_tonnes_processed

    def get_tdu_throughput(self):
        return self.tdu_throughput

    def print_unitnum(self):
        print(self.tdu_unit)