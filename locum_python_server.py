import pandas as pd
import mysql.connector
import datetime as dt
from datetime import date, datetime

def get_MySQL_database_Connector():
    cnx = mysql.connector.connect(user='root', password='password',
                              host='127.0.0.1',
                              database='AD_Locum')
    cursor = cnx.cursor()
    return cnx, cursor

def compute_Yesterday_Daily_AverageRate_And_UpdateTable(MySQLcnx, MySQLcursor):
    tz_Singapore = dt.timezone(dt.timedelta(hours=8))
    ytd_date = datetime.now(tz=tz_Singapore).date() - dt.timedelta(days=1)
    weekend_flag = None
    if(ytd_date.weekday() == 5 or ytd_date.weekday() == 6):
        weekend_flag = True
    else:
        weekend_flag = False
    ytd_date_string = ytd_date.strftime('%Y-%m-%d')
    ytd_date_start_time_string = ytd_date_string + " 00:00:00"
    ytd_date_end_time_string = ytd_date_string + " 23:59:59"
    query = ("SELECT AVG(jp.rate_per_hour) FROM AD_Locum.job_post jp WHERE (start_date_time between '{0}' and '{1}') AND (jp.status = 2 OR jp.status = 3 OR jp.status = 4)".format(ytd_date_start_time_string, ytd_date_end_time_string))
    result_dataFrame = pd.read_sql(query, cnx)
    update_average_daily_rate_sql = None
    if (weekend_flag == True):
        update_average_daily_rate_sql = ("UPDATE Average_Daily_Rate SET average_daily_rate_weekend=%s WHERE date=%s")
    else:
        update_average_daily_rate_sql = ("UPDATE Average_Daily_Rate SET average_daily_rate_weekday=%s WHERE date=%s")
    val = (result_dataFrame.iloc[0,0], ytd_date_string)
    MySQLcursor.execute(update_average_daily_rate_sql, val)
    MySQLcnx.commit()

def compute_MA_Daily_Average_Rate(MySQLcnx, MA_days):
    tz_Singapore = dt.timezone(dt.timedelta(hours=8))
    ytd_date = datetime.now(tz=tz_Singapore).date() - dt.timedelta(days=1)
    date_TS_start_date = ytd_date - dt.timedelta(days=(MA_days-1))
    date_TS_start_date = date_TS_start_date.strftime('%Y-%m-%d') 
    date_TS_end_date = ytd_date
    date_TS_end_date = date_TS_end_date.strftime('%Y-%m-%d')
    ts_wkday_pastXdays_query = ("SELECT AVG(ar.average_daily_rate_weekday) FROM AD_Locum.Average_Daily_Rate ar WHERE ar.date BETWEEN '{0}' AND '{1}'".format(date_TS_start_date, date_TS_end_date))
    ts_wkend_pastXdays_query = ("SELECT AVG(ar.average_daily_rate_weekend) FROM AD_Locum.Average_Daily_Rate ar WHERE ar.date BETWEEN '{0}' AND '{1}'".format(date_TS_start_date, date_TS_end_date))
    ts_weekday_result_dataFrame = pd.read_sql(ts_wkday_pastXdays_query, MySQLcnx)
    print(ts_weekday_result_dataFrame.iloc[0,0])
    ts_weekend_result_dataFrame = pd.read_sql(ts_wkend_pastXdays_query, MySQLcnx)
    print(ts_weekend_result_dataFrame.iloc[0,0])
    return ts_weekday_result_dataFrame, ts_weekend_result_dataFrame

def create_Today_Date_Row_In_Table(MySQLcnx, MySQLcursor):
    tz_Singapore = dt.timezone(dt.timedelta(hours=8))
    database_table_new_row_date = datetime.now(tz=tz_Singapore).date()
    next_row_sql = ("INSERT INTO Average_Daily_Rate (date) VALUES (%s)")
    val = (database_table_new_row_date.strftime('%Y-%m-%d'),)
    MySQLcursor.execute(next_row_sql, val)
    MySQLcnx.commit()

def update_14_MA_column(MySQLcnx, MySQLcursor, weekday_dataframe, weekend_dataframe):
    #Update column weekday_14_MA
    database_table_today_date = datetime.now(tz=tz_Singapore).date()
    update_MA_weekday_sql = ("UPDATE Average_Daily_Rate SET weekday_14_MA=%s WHERE date=%s")
    val = (weekday_dataframe.iloc[0,0], database_table_today_date.strftime('%Y-%m-%d'))
    MySQLcursor.execute(update_MA_weekday_sql, val)
    MySQLcnx.commit()
    update_MA_weekend_sql = ("UPDATE Average_Daily_Rate SET weekend_14_MA=%s WHERE date=%s")
    val = (weekend_dataframe.iloc[0,0], database_table_today_date.strftime('%Y-%m-%d'))
    MySQLcursor.execute(update_MA_weekend_sql, val)
    MySQLcnx.commit()

def update_28_MA_column(MySQLcnx, MySQLcursor, weekday_dataframe, weekend_dataframe):
    #Update column weekday_14_MA
    database_table_today_date = datetime.now(tz=tz_Singapore).date()
    update_MA_weekday_sql = ("UPDATE Average_Daily_Rate SET weekday_28_MA=%s WHERE date=%s")
    val = (weekday_dataframe.iloc[0,0], database_table_today_date.strftime('%Y-%m-%d'))
    MySQLcursor.execute(update_MA_weekday_sql, val)
    MySQLcnx.commit()
    update_MA_weekend_sql = ("UPDATE Average_Daily_Rate SET weekend_28_MA=%s WHERE date=%s")
    val = (weekend_dataframe.iloc[0,0], database_table_today_date.strftime('%Y-%m-%d'))
    MySQLcursor.execute(update_MA_weekend_sql, val)
    MySQLcnx.commit()

def main():
    cnx, cursor = get_MySQL_database_Connector()
    compute_Yesterday_Daily_AverageRate_And_UpdateTable(cnx,cursor)
    create_Today_Date_Row_In_Table(cnx, cursor)
    #Compute 14 Days Weekday & Weekend Rates Moving Average
    ma_14_wkday_dataframe, ma_14_wkend_dataframe= compute_MA_Daily_Average_Rate(cnx, 14)
    update_14_MA_column(cnx, cursor, ma_14_wkday_dataframe, ma_14_wkend_dataframe)
    ma_28_wkday_dataframe, ma_28_wkend_dataframe= compute_MA_Daily_Average_Rate(cnx, 28)
    update_28_MA_column(cnx,cursor, ma_28_wkday_dataframe, ma_28_wkend_dataframe)
    # print(scheduler.print_jobs())

# run the server
if __name__ == '__main__':
    counter = 0
    while counter < 10:
        tz_Singapore = dt.timezone(dt.timedelta(hours=8))
        cnx, cursor = get_MySQL_database_Connector()
        seed_query = "SELECT COUNT(ADR.date) FROM Average_Daily_Rate ADR" 
        seed_result_dataFrame = pd.read_sql(seed_query, cnx)
        print("Overhere lah deh!!!")
        print(type(seed_result_dataFrame.iloc[0,0]))
        if(seed_result_dataFrame.iloc[0,0] == 0):
            print("True It is 0")
            start_date = date.today() - dt.timedelta(days=1)
            start_date = start_date.strftime('%Y-%m-%d')
            seed_sql = ("INSERT INTO Average_Daily_Rate (date) VALUES (%s)")
            val = (start_date,)
            cursor.execute(seed_sql, val)
            cnx.commit()
        else:
            print("False It it is not 0")
        main()
        counter += 1


