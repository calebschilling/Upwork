import csv
import datetime
import time
import pandas as pd
import datetime
from sqlalchemy import create_engine
from configparser import ConfigParser
import re
import time
import logging
import psycopg2
import pytz
import os
import zeep
import five9_session
import requests


# *----------------- CONFIG PARSER------------*

# Load configuration from config.ini
config = ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

# *----------------- FUNCTIONS ---------------*
# *Connection Establishment*

def connect_to_postgres(): # Connect to the PostgreSQL database server: return conn, db_cursor
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=config.get('Database', 'db_name'),
        user=config.get('Database', 'db_username'),
        password=config.get('Database', 'db_password'),
        host=config.get('Database', 'db_host'),
        port=config.get('Database', 'db_port'),
        sslmode='require'
    )

    # Create a cursor object
    db_cursor = conn.cursor()
    return conn, db_cursor

def connect_to_five9(): # Connect to the Five9 API: return five9_client
    # Create a Five9 client
    five9_client = five9_session.Five9Client()
    return five9_client


#  *TEMPERORARY VARIABLE*

# *Transformation*

def transform_headers(headers): # Transform headers for postgres
    for header in headers:
        modified_header = header.replace(' ', '_').replace('(', '').replace(')', '')
        # put modified_header in place of current header
        headers[headers.index(header)] = modified_header

    return headers

def transform_time_to_seconds(report_time): # Convert time to seconds.  00:01:30 --> 90 (hours:minutes:seconds)
    # Regular expression to match the hour, minute, and second parts
    pattern = r"(\d{2}):(\d{2}):(\d{2})$"

    # Search for the pattern in the text
    match = re.search(pattern, report_time)

    if match:
        hours = int(match.group(1))
        minutes = int(match.group(2))
        seconds = int(match.group(3))

        # Convert hours and minutes to seconds and add the seconds
        total_seconds = (hours * 3600) + (minutes * 60) + seconds
        if hours > 0:
            pass
        return total_seconds

def transform_timestamp(report_timestamp): # tranform timestamp to YYYY-MM-DD HH:MM:SS
    from datetime import datetime, timedelta # The original import at the top of the file was throwing off criteria_datetime_start and criteria_datetime_end, hence why i put it here
    # Parse the input date string
    parsed_date = datetime.strptime(report_timestamp, "%a, %d %b %Y %H:%M:%S")
    parsed_date = parsed_date + timedelta(hours=2)

    # Format the date in the desired output format
    formatted_date = parsed_date.strftime("%Y-%m-%d %H:%M:%S")

    return formatted_date

def transform_timestamp_four_hours(most_recent_timestamp): 
    from datetime import datetime, timedelta 

    # take most_recent_timestamp and subtract 4 hours from it
    subtract_four_hours = timedelta(hours=4)
    minus_four_hour_timestamp = most_recent_timestamp - subtract_four_hours
    logging.info(f"Most recent timestamp: {most_recent_timestamp}")
    logging.info(f"Most recent timestamp - 4 hours: {minus_four_hour_timestamp}")    
    print(minus_four_hour_timestamp)
    return minus_four_hour_timestamp

def transform_most_recent_timestamp_to_pst(most_recent_timestamp):
    from datetime import datetime, timedelta

    # take most_recent_timestamp and subtract 4 hours from it
    subtract_four_hours = timedelta(hours=2)
    minus_four_hour_timestamp = most_recent_timestamp - subtract_four_hours
    
    return minus_four_hour_timestamp

def transform_report(report_result):
    original_headers = report_result['header']['values']['data'] # returns a list of the original column names/headers
    headers = transform_headers(original_headers) # returns a list of the column names/headers with spaces replaced with underscores for postgres

    data = [row['values']['data'] for row in report_result['records']]
    # get length of data
    length_of_data = len(data)
    logging.info(f"Rows returned: {length_of_data}")
    
    transformed_data = [] # this will be a list of lists, each list is a row of data that has been manipulated

    headers_to_convert_to_seconds = ["CALL_TIME", "BILL_TIME_ROUNDED", "IVR_TIME", "QUEUE_WAIT_TIME", "RING_TIME", "TALK_TIME", "HOLD_TIME", "PARK_TIME", "AFTER_CALL_WORK_TIME", "SPEED_OF_ANSWER", "HANDLE_TIME"]
    headers_to_convert_to_int = ["Survey.Q1", "Survey.Q2", "Survey.Q3", "CALLS_COMPLETED_IN_IVR", "ABANDONED", "HOLDS", "TRANSFERS", "DNIS", "ANI", "CALLS", "SERVICE_LEVEL"]


    for row_data in data:

        # Process the rest of the columns
        for i, (header, value) in enumerate(zip(headers[0:], row_data[0:])):

            if value is not None:
                try:
                    # set call_id and IVR_Path
                    if header == 'CALL_ID':
                        call_id = int(value) # convert to int
                        row_data[i] = call_id # Update the value in the row_data
                    # elif header == 'IVR_PATH':
                    #     IVR_path = value
                    #     row_data[i] = ""                    
                        # get rid of IVR_Path in row_data and IVR_PATH
                    elif header in headers_to_convert_to_seconds:
                        if header == 'SPEED_OF_ANSWER':
                            stripped_value = value.split('.')[0]
                            new_time = transform_time_to_seconds(stripped_value)
                            row_data[i] = new_time
                        else:
                            new_time = transform_time_to_seconds(value)
                            row_data[i] = new_time  # Update the value in the row_data and get rid of old value

                    # Convert timestamp to YYYY-MM-DD HH:MM:SS
                    # is value in this type of format 'Mon, 20 Nov 2023 14:38:30'
                    elif header == 'TIMESTAMP':
                        new_timestamp = transform_timestamp(value)
                        row_data[i] = new_timestamp
                    elif header in headers_to_convert_to_int: # if header is a survey question, convert to int and if you can't convert to int, set to None
                        try:
                            row_data[i] = int(value)
                        except ValueError:
                            row_data[i] = None
                        except Exception as e:
                            row_data[i] = None
                    elif header == 'ABANDON_RATE':
                        percentage_value = float(value[:-1]) / 100.0  # Convert to float and adjust for percentage
                        row_data[i] = percentage_value
                    elif value == '[None]':
                        value = None
                        row_data[i] = value
                    else:
                        pass

                except IndexError as e:
                    logging.error(e)
                    pass
                except ValueError as e: # if value is in milliseconds, don't add it to the database
                    logging.error(e)
                    pass
                except Exception as e:
                    logging.error(e)



        # Done processing the single row in data

        transformed_data.append(row_data)

        # # ETL IVR PATH to table_ivr_detail
        # if call_id is not None and IVR_path is not None:
        #     extract_IVR_path(call_id, IVR_path, db_cursor)

        call_id = None
        IVR_Path = None

    return headers, transformed_data

# *Timezone Offset*

def timezone_offset():
    # Get the current time in the 'America/Chicago' (CST) time zone
    chicago_tz = pytz.timezone('America/Chicago')
    current_time_utc = datetime.datetime.utcnow() 
    # current time in pst
    
    current_time_chicago = current_time_utc.replace(tzinfo=pytz.utc).astimezone(chicago_tz)


    # Get the timezone offset for display with colons
    timezone_offset_seconds = current_time_chicago.utcoffset().total_seconds()
    hours, remainder = divmod(abs(timezone_offset_seconds), 3600)
    minutes = remainder // 60
    sign = '-' if timezone_offset_seconds < 0 else '+'

    timezone_offset = f"{sign}{int(hours):02}:{int(minutes):02}"
    # timezone_offset = int(timezone_offset)
    return timezone_offset

# *Extraction*

def extract_report(criteria_datetime_start,criteria_datetime_end,report_folder,report_name, first_run): # Extract the report from five9

    if first_run == True:

        start = criteria_datetime_start.strftime("%Y-%m-%dT%H:%M:%S.000") 
        end = criteria_datetime_end.strftime("%Y-%m-%dT%H:%M:%S.000") 

        report_criteria = {
        "time": {
            "start": start, 
            "end": end, 
        },
        }
    else:    
            
        start = criteria_datetime_start.strftime("%Y-%m-%dT%H:%M:%S.000") 
        end = criteria_datetime_end.strftime("%Y-%m-%dT%H:%M:%S.000") 

        report_criteria = {
        "time": {
            "start":start, 
            "end": end, 
        },
        }

    try:
        report_run_id = five9_client.service.runReport(
            folderName=report_folder, reportName=report_name, criteria=report_criteria
        )
        print(f"Report Id Requested: {report_run_id}")
        logging.info(f"Report Id Requested: {report_run_id}")
    except Exception as e:
        logging.error(e)
        print(e)

    # Check to see if the report has finished running.
    # IMPORTANT: failure to pause for some time between each check to see if the
    # report has run can cause you to exceed your API limits.
    report_running = True
    checks = 0
    while report_running is True:
        report_running = five9_client.service.isReportRunning(report_run_id, timeout=10)
        if report_running is True:
            print(f"Still Running ({checks})", end="\r")
            logging.info(f"Still Running ({checks})")
            # IMPORTANT - invoking this method on a loop for a long-running report
            # WILL burn up your API rate limits, put some time between requests
            time.sleep(5)

    print("DONE Running")
    logging.info("DONE Running")

    # Csv result set is faster to obtain, less verbose response body
    # reportResultCsv = five9_client.getReportResultCsv(report_run_id)
    # csv_lines = reportResultCsv.split('\n')

    # Get the report result in JSON format
    reportResult = five9_client.service.getReportResult(report_run_id)
    reportResultCSV = five9_client.service.getReportResultCsv(report_run_id)
    # log the length of the report result
    
    return reportResult

# *Load*

def load_to_postgres(headers, transformed_data, db_cursor):
    # Define the PostgreSQL connection URL
    insert_query = "INSERT INTO public.\"callDetail\" ({}) VALUES ({})".format(', '.join(['\"' + col + '\"' for col in headers]), ', '.join(['%s']*len(headers)))

    db_cursor.executemany(insert_query, transformed_data)
    conn.commit()
    print('write to postgres complete')

# *Query*

def return_query(sql_query): # Return query results
    db_cursor.execute(sql_query)
    query_results = db_cursor.fetchall()
    return query_results

def delete_previous_four_hours(minus_four_hour_timestamp):

    try:
        sql_query_delete = "DELETE FROM public.\"callDetail\" WHERE \"TIMESTAMP\" >= '{}'".format(minus_four_hour_timestamp)
        db_cursor.execute(sql_query_delete)
        conn.commit()
        logging.info(f"Deleted previous four hours of data greater than {minus_four_hour_timestamp}")
        print('delete complete')
    except Exception as e:
        logging.error(f"Logging error in DELETE function: {e}")
        
# *Beginning of Week*
def beginning_of_week(): # Get beginning of week timestamp
    from datetime import datetime, timedelta

    # Get the current date and time
    now = datetime.now()

    # Calculate the difference between the current day and the beginning of the week (Monday)
    days_to_subtract = (now.weekday() - 6) % 7
    beginning_of_week = now - timedelta(days=days_to_subtract, hours=now.hour, minutes=now.minute, seconds=now.second, microseconds=now.microsecond)

    # Set the time part to midnight
    beginning_of_week = beginning_of_week.replace(hour=0, minute=0, second=0, microsecond=0)

    timestamp_formatted = beginning_of_week

    # print(timestamp_formatted)
    return timestamp_formatted

# *Run Report*

def run_report(report_folder, report_name): # Run report using query to get most recent timestamp for start / end times
    most_recent_call_id_and_timestamp = "SELECT \"CALL_ID\", \"TIMESTAMP\" FROM public.\"callDetail\" WHERE DATE(\"TIMESTAMP\") = CURRENT_DATE ORDER BY \"CALL_ID\" DESC LIMIT 1;"
    query_results = return_query(most_recent_call_id_and_timestamp)
    most_recent_call_id_and_timestamp = query_results
    # Get initial timestamp from postgres
    # sql_query_most_recent_timestamp = "SELECT MAX(\"TIMESTAMP\") - INTERVAL '2 hours' AS adjusted_timestamp FROM \"callDetail\";" # pst most_recent
    sql_query_most_recent_timestamp = "SELECT MAX(\"TIMESTAMP\") + INTERVAL '1 second' FROM \"callDetail\";" # pst most_recent
    query_results  = return_query(sql_query_most_recent_timestamp)
    most_recent_timestamp = query_results[0][0]
    
    # Get now from postgres
    sql_query_now = "SELECT now()" # postgres is in CST
    query_results = return_query(sql_query_now)
    now_timestamp = query_results[0][0]

    if most_recent_timestamp is None: # if table is empty start at midnight and end at now 
        
        # get timestamp for beginning of week
        start_of_week = beginning_of_week()

        criteria_datetime_start = start_of_week
        criteria_datetime_end = now_timestamp

        first_run = True # if the table is totally empty, this is the first run

        report_result = extract_report(criteria_datetime_start, criteria_datetime_end, report_folder, report_name,first_run)
        return report_result

    elif query_results is not None: # start at most_recent + 1 second and end at now cst
    
        # send most_recent_timestamp through transform_timestamp_four_hours
        minus_four_hour_timestamp = transform_timestamp_four_hours(most_recent_timestamp)

        # delete previous four hours of data
        delete_previous_four_hours(minus_four_hour_timestamp)
        
        pst_most_recent_minus_four_hours = transform_most_recent_timestamp_to_pst(minus_four_hour_timestamp)

        criteria_datetime_start = pst_most_recent_minus_four_hours

        criteria_datetime_end = now_timestamp

        first_run = False # if the table is not empty, this is not the first run

        report_result = extract_report(criteria_datetime_start, criteria_datetime_end, report_folder, report_name, first_run)
        return report_result


def nty_notification(notification_message):
    requests.post("https://ntfy.sh/Nitor_Ops", 
        data=notification_message.encode(encoding='utf-8'))

# *----------------- MAIN --------------------*

# *ESTABLISH CONNECTIONS*

# Connect to the PostgreSQL database server
try:
    conn, db_cursor = connect_to_postgres()
    logging.info("Connected to the PostgreSQL database server")
except (Exception, psycopg2.DatabaseError) as error:
    logging.error(error)
except Exception as e:
    logging.error(e)

# Connect to the Five9 API
try:
    five9_client = connect_to_five9()
    logging.info("Connected to the Five9 API")
except Exception as e:
    logging.error(e)

# *EXTRACT REPORT*

# Report credentials
report_folder = "Shared Reports"
report_name = "ETL Call Detail"

# Extract the report
try:
    report_result = run_report(report_folder, report_name)
    logging.info("Report Successfully Extracted")
except Exception as e:
    logging.error(e)
    nty_notification("Five9 report failed to extract the report")

# *TRANSFORM REPORT*

headers, transformed_data = transform_report(report_result)
# tranformed_data length
length_of_transformed_data = len(transformed_data)
logging.info(f"Rows tranformed and returned: {length_of_transformed_data}")

# *LOAD TO POSTGRES*

try:
    load_to_postgres(headers, transformed_data, db_cursor)
    logging.info("Data Successfully Loaded to Postgres")
    # nty_notification("Five9 report successfully loaded to Postgres")
except Exception as e:
    logging.error(e)
    nty_notification("Five9 report failed to load to Postgres")
except psycopg2.Error as e:
    logging.error(e)
    nty_notification("Five9 report failed to load to Postgres")

