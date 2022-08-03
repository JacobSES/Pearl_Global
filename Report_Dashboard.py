import datetime
from re import S
from unicodedata import decimal
import numpy as np
import pandas as pd
import streamlit as st
import sqlalchemy as sal
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from Report_TDU import TDU
import pyodbc
import snowflake.connector

class DASHBOARD:

    def __init__(self, TDU_LIST):
        ##page configuration
        # with st.echo(code_location='below'):
            self.today = (str)(datetime.date.today())
            self.yesterday = (str)(datetime.date.today() - datetime.timedelta(days = 1))
            self.start_day = (str)(datetime.date.today() - datetime.timedelta(days = 7))
            self.last_week_start = (str)(datetime.date.today() - datetime.timedelta(days = 14))
            self.unit_list = ["TDU02", "TDU03", "TDU04"]
            self.time_range = ["Day to Day", "Month to Month", "Year to Year"]
            self.performance_target = {"Availability": 0, "Throughput": 0, "Tonnes Processed": 0}
            self.weekly_performance_df = {}
            st.set_page_config(page_title = "ENTYRE Plant Master Production", 
                                page_icon = ":bar_chart:", 
                                initial_sidebar_state="collapsed",
                                layout="wide")
            ##this is header
            t1,t2 = st.columns((0.2, 1))
            t1.image("images/Pearl_Logo.png", width = 250)
            t2.title("Operational Performace Dashboard")
            t2.markdown(" **tel:** (08)6252 8135 **| website:** https://pearlglobal.com.au **| email:** info@pearlglobal.com.au")
            
            #Select data
            self.tdu_select = st.selectbox("Choose TDU Unit:", self.unit_list, help = "Filter Report to show only one TDU unit")
            date_sidebar = st.sidebar.selectbox("Select Time Range", self.time_range, help= "Filter Time Range") #### NEED TO WORK ON THIS

            # ##Set the tdu performance target
            with st.sidebar.form(key = "Target_Form"):
                
                target_availability = st.number_input("Target Availability (%)", step=1, value=0)
                target_throughput = st.number_input("Target Throughput (t/hr)", step=1, value=0)
                target_tonnes_procssed = st.number_input("Target Tonnes Procssed (t)", step=1, value=0)
                target_submit = st.form_submit_button(label = "Submit")
                if target_submit == True:
                    self.performance_target["Availability"] = target_availability
                    self.performance_target["Throughput"] = target_throughput
                    self.performance_target["Tonnes Processed"] = target_tonnes_procssed
                            
            tdu_index = self.get_tdu_index(self.tdu_select)

            ##today performance
            m1,m2,m3,m4,m5 = st.columns((1,1,1,1,1))
            self.weekly_performance_df = self.read_weekly_performance_df(self.start_day, self.today)
            self.lastWeek_performance_df = self.read_weekly_performance_df(self.last_week_start, self.start_day)
            pd.to_datetime(self.weekly_performance_df[tdu_index]["DATE"])
            print(self.weekly_performance_df[tdu_index].dtypes)
            print(self.weekly_performance_df[tdu_index].loc[self.today])
            # Today_data = self.weekly_performance_df[tdu_index].loc[self.today]
            # Yesterday_data = self.weekly_performance_df[tdu_index].loc[self.yesterday]

            # m1.write("")
            # m2.metric(label = self.tdu_select + " Availability", value = str(round(Today_data["Availability"], 2)) + "%", 
            #         delta = str(round(Today_data["Availability"] - Yesterday_data["Availability"], 2)) + "% Compared to yesterday")
            # m3.metric(label = self.tdu_select + " Throughput", value = str(round(Today_data["Throughput"], 2)) + " t/hr", 
            #         delta = str(round(Today_data["Throughput"] - Yesterday_data["Throughput"], 2)) + "kg/hr Compared to yesterday")
            # m4.metric(label = self.tdu_select + " Tonnes Procssed", value = str(round(Today_data["Tonnes_Processed"], 2)) + "tonnes", 
            #         delta = str(round(Today_data["Tonnes_Processed"] - Yesterday_data["Tonnes_Processed"], 2)) + "kg/hr Compared to yesterday")
            # m5.write("")

            # g1,g2 = st.columns((1,1))
            # fig1 = self.plotly_availability_graph(self.weekly_performance_df[tdu_index].loc[:,["Availability"]])
            # fig2 = self.plotly_availability_chart()
            # g1.plotly_chart(fig1, use_container_width=True)
            # g2.plotly_chart(fig2, use_container_width=True)

            # h1,h2 = st.columns((1,1))
            # fig3 = self.plotly_tonnes_processed_chart(self.weekly_performance_df)
            # fig4 = self.plotly_tonnes_processed_pie(self.weekly_performance_df, self.lastWeek_performance_df)
            # h1.plotly_chart(fig3, use_container_width=True)
            # h2.plotly_chart(fig4, use_container_width=True)

            # k1,k2 = st.columns((1,1))
            # fig5 = self.plotly_throughput_graph(self.weekly_performance_df[tdu_index].loc[:,["Throughput"]])
            # fig6 = self.plotly_throughput_by_TDU_graph(self.weekly_performance_df, self.lastWeek_performance_df)
            # k1.plotly_chart(fig5, use_container_width=True)
            # k2.plotly_chart(fig6, use_container_width=True)

            # # t1 = st.columns(1,0)
            # fig7 = self.plotly_availability_tonnes_procssed_graph(self.weekly_performance_df)
            # st.plotly_chart(fig7, use_container_width = True)

    def get_tdu_index(self, tdu_select):

        if tdu_select == "TDU02":
            return 0
        elif tdu_select == "TDU03":
            return 1         
        elif tdu_select == "TDU04":
            return 2       

    def get_tdu_name(self, tdu_select):

        if tdu_select == 0:
            return "TDU02"
        elif tdu_select == 1:
            return "TDU03"        
        elif tdu_select == 2:
            return "TDU04"

    def read_weekly_performance_df(self, start_day, end_day):
        weekly_performance_df = {}
        weekly_performance_query = f"SELECT * FROM dbo.PERFORMANCE_REPORT WHERE DATE BETWEEN '{start_day}' AND '{end_day}'" ##SET THE START DATE
        index = 0
        for i in self.unit_list:
            weekly_performance_df[index] = self.read_from_sql(i, weekly_performance_query)    
            index+=1
        return weekly_performance_df

    def read_from_sql(self, database, query):
        
        conn = self.init_connection()
        sql_query = self.run_query(query, conn)
        df = pd.DataFrame(sql_query)        
        pd.to_datetime(df["DATE"])
        # df.set_index("DATE", inplace= True)
        return df

    @st.experimental_singleton
    def init_connection(self):
        return snowflake.connector.connect(**st.secrets["snowflake"])
        
    @st.experimental_memo(ttl=600)
    def run_query(self, query, conn):
        with conn.cursor() as cur:
            cur.execute(query)
        return cur.fetchall()

    def plotly_availability_graph(self, performance_df):
        fig_df = performance_df
        fig_df["Target"] = self.performance_target["Availability"]
        fig = px.line(fig_df, x=fig_df.index, y=[fig_df["Availability"], fig_df["Target"]])
        fig.update_traces(marker_color = '#264653')
        fig.update_yaxes(range=[0, 100])
        fig.update_layout(title_text = f"{self.tdu_select} Availability Trend" ,title_x=0, yaxis_title = "Availability", xaxis_title = "Date")
        return fig

    def plotly_availability_chart(self):
        ###green if its below 70
        fig = go.Figure(data=[go.Table(header=dict(values=['TDU1', 'TDU2', 'TDU3', 'TDU4', 'Total'], 
                                                    font = dict(size = 18, color = "white"), fill_color = '#264653', line_color = 'rgba(255,255,255,0.2)', align = 'center', height = 35),
                        cells=dict(values=[[75, 67], [75, 86],[79, 46],[78,70],[77,67]],
                                    font = dict(size = 16, color = 'black'), height = 30, align = "center") ## need to create availability df and change color based on value                        
                        )])
        map_color = {"YES":"green", "NO":"red", "BORDERLINE":"blue"}
        fig.update_layout(title_text = "TDU Weekly Availability")
        return fig

    def plotly_tonnes_processed_chart(self, df_list):
        fig_df = pd.concat([df_list[0]["Tonnes_Processed"], df_list[1]["Tonnes_Processed"], df_list[2]["Tonnes_Processed"]], axis=1, keys = self.unit_list)
        fig_df["Target"] = self.performance_target["Tonnes Processed"]
        fig = px.line(fig_df, x=fig_df.index, y=[fig_df[self.unit_list[0]], fig_df[self.unit_list[1]], fig_df[self.unit_list[2]], fig_df['Target']])
        fig.update_traces(marker_color = '#264653')
        fig.update_layout(title_text = "Tonnes Processed Trend" ,title_x=0, yaxis_title = "Tonnes Processed", xaxis_title = "Date")
        return fig

    def plotly_tonnes_processed_pie(self, df_list, df_list_lastWeek):
        ##current week vs last week.
        tonnes_processed_weekly = {"TDU02": 0, "TDU03": 0 , "TDU04": 0}
        tonnes_processed_weekly_last = {"TDU02": 0, "TDU03": 0 , "TDU04": 0}
        index = 0
        for i in df_list:
            tonnes_processed_weekly[self.unit_list[index]] = round(df_list[i]["Tonnes_Processed"].mean(), 2)
            tonnes_processed_weekly_last[self.unit_list[index]] = round(df_list_lastWeek[i]["Tonnes_Processed"].mean(), 2)
            index+=1

        fig = make_subplots(1, 2, specs=[[{'type':'domain'}, {'type':'domain'}]], subplot_titles=['Last Week', 'Current Week'])
        fig.add_trace(go.Pie(labels = list(tonnes_processed_weekly_last.keys()), values = list(tonnes_processed_weekly_last.values()), hole = .6), 1, 1)
        fig.add_trace(go.Pie(labels = list(tonnes_processed_weekly.keys()), values = list(tonnes_processed_weekly.values()), hole = .6), 1, 2)
        fig.update_layout(title_text = "TDU Weekly Tonnes Processed")
        return fig

    def plotly_throughput_graph(self, performance_df):
        ##current week vs last week.
        fig_df = performance_df
        fig_df["Target"] = self.performance_target["Throughput"]
        fig = px.line(fig_df, x=fig_df.index, y=[fig_df["Throughput"], fig_df["Target"]])
        fig.update_traces(marker_color = '#264653')
        fig.update_layout(title_text = f"{self.tdu_select} Throughput (kg/hr)" ,title_x=0, yaxis_title = "Throughput (kg/hr)", xaxis_title = "Date")
        return fig
    
    def plotly_throughput_by_TDU_graph(self, performance_df, performance_df_lastWeek):
        throughput_weekly_average = {"TDU02": 0, "TDU03": 0 , "TDU04": 0, "Average": 0}
        throughput_weekly_average_lastWeek = {"TDU02": 0, "TDU03": 0 , "TDU04": 0, "Average": 0}
        weekly_total_average = 0
        lastWeekly_total_average = 0
        for i in performance_df:
            # average = performance_df[i].loc[:,["Throughput"]].mean()
            average = performance_df[i]["Throughput"].mean()
            average_lastWeek = performance_df_lastWeek[i]["Throughput"].mean()
            throughput_weekly_average[self.get_tdu_name(i)] = average.round(decimals = 1)
            throughput_weekly_average_lastWeek[self.get_tdu_name(i)] = average_lastWeek.round(decimals = 1)
            weekly_total_average += average
            lastWeekly_total_average += average_lastWeek

        throughput_weekly_average["Average"] = (weekly_total_average/3).round(decimals = 1)
        throughput_weekly_average_lastWeek["Average"] = (lastWeekly_total_average/3).round(decimals = 1)

        fig = make_subplots(1, 2, specs=[[{'type':'domain'}, {'type':'domain'}]], subplot_titles=['Last Week', 'Current Week'])
        fig.add_trace(go.Bar(x = list(throughput_weekly_average.keys()), y = list(throughput_weekly_average.values()), name = "Current Week", text = list(throughput_weekly_average.values()), textposition = "inside"))
        fig.add_trace(go.Bar(x = list(throughput_weekly_average_lastWeek.keys()), y = list(throughput_weekly_average_lastWeek.values()), name = "Previous Week", text = list(throughput_weekly_average_lastWeek.values()), textposition = "inside"))
        
        return fig

    def plotly_availability_tonnes_procssed_graph(self, performance_df):
        index_list = []
        availability_list = {"TDU02": [], "TDU03": [] , "TDU04": []}
        tonnes_processed_list = {"TDU02": [], "TDU03": [] , "TDU04": []}

        for i in performance_df:
            for index,row in performance_df[i].iterrows():
                availability_list[self.get_tdu_name(i)].append(row["Availability"])
                tonnes_processed_list[self.get_tdu_name(i)].append(row["Tonnes_Processed"])

        index_list = list(performance_df[0].index.values)
        
        availability_average = [0] * len(index_list)
        tonnes_procssed_sum = [0] * len(index_list)
        for (avail, tonnes) in zip(availability_list.values(), tonnes_processed_list.values()):
            for i in range(len(index_list)):
                availability_average[i] += avail[i]
                tonnes_procssed_sum[i] += tonnes[i]

        ##calculate availabilty
        for i in range(len(availability_average)):
            availability_average[i] = availability_average[i]/3

        fig_df = pd.DataFrame(data = {"Availability":availability_average, "Tonnes Processed": tonnes_procssed_sum}, index = index_list)
        fig = make_subplots(specs=[[{"secondary_y": True}]])

        fig.add_trace(go.Scatter(x = fig_df.index, y = fig_df["Tonnes Processed"], name = "Tonnes Processed", marker = dict(color = 'royalblue', size = 8), line = dict(color = 'royalblue', width =3)), secondary_y = False)
        fig.add_trace(go.Scatter(x = fig_df.index, y = fig_df["Availability"], name = "Availability", line = dict(color = 'red', width =4)), secondary_y = True)
        fig.update_layout(title_text = "Availability vs Tonnes Processed" ,title_x=0 , xaxis_title = "Date")
        fig.update_yaxes(title_text="<b>Tonnes Processed</b>", secondary_y=False)
        fig.update_yaxes(title_text="<b>Availability</b>", secondary_y=True)
        fig.update_traces(marker_color = '#264653')

        return fig