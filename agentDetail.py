import datetime
from datetime import datetime, time, timedelta
from email import header
from json import load
from wsgiref import headers
from five9 import Five9
import re
import schedule
import time
import logging
import os 
from configparser import ConfigParser
import pytz
import psycopg2
from sqlalchemy import table
import requests
from send_telegram_message import send_telegram_message
#  -------------------------- CONFIGURATION --------------------------
# Load configuration from config.ini
config = ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'config.ini')
config.read(config_path)

# Replace with your bot's token and your chat ID
bot_token = config.get('Telegram', 'token')
chat_id = config.get('Telegram', 'sarah_chat_id')
message = 'Hey Sarah! Are you able to log out a supervisor user so we can update the agent stats on Metabase? Thanks 😄'


# -------------------------- LOGGING --------------------------
# LOG_FILE_PATH = "/home/etl_user/logs/five9Statistics.log"
# LOG_FILE_PATH = "/home/etl_user/five9Statistics/output.log"

# # Configure logging
# logging.basicConfig(
#     filename=LOG_FILE_PATH,
#     level=logging.INFO,
#     format='%(asctime)s [%(levelname)s]: %(message)s',
#     datefmt='%b. %d, %Y %I:%M:%S %p'  # Custom date and time format
# )

# *--------------------- FUNCTIONS ---------------------*

# *Time* 
def get_timezone_hour_difference(timezone_name): # Account for daylight savings changes for timezone offset in Five9
    tz = pytz.timezone(timezone_name)
    current_time_utc = datetime.utcnow()
    current_time = tz.fromutc(current_time_utc)
    hour_difference = current_time.hour - current_time_utc.hour
    # print(f"Timezone hour difference: {hour_difference}")
    return hour_difference

def get_current_time_in_cst():
    cst = pytz.timezone('US/Central')
    current_time = datetime.now(cst)
    return current_time.replace(microsecond=0)

def calculate_state_timer(state_since):
    # get current time in cst
    current_time = get_current_time_in_cst()
    time_difference = current_time - state_since
    time_difference_str = str(time_difference)
    return time_difference_str

# *Connections*
def connect_to_five9():
    # Five9 API credentials
    five9_username = config.get('Five9', 'username')
    five9_password = config.get('Five9', 'password')

    # Get the timezone offset in hours
    timezone_offset_hour = get_timezone_hour_difference('America/Chicago')

    # Create a Five9 client
    five9_client = Five9(five9_username, five9_password)
    five9_client.rolling_period='Today'
    five9_client.time_zone_offset= timezone_offset_hour
    five9_client.statistics_range='CurrentDay'

    return five9_client

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

# *Transform*

def transform_ms_to_seconds(milliseconds_value): # Convert milliseconds to seconds
    # convert string to integer
    try:
        milliseconds_value = int(milliseconds_value)
        seconds_value = milliseconds_value / 1000
        return seconds_value
    except ValueError as e:
        # Log the error and continue without returning anything
        logging.error(f"Failed to convert {milliseconds_value} to integer. Returned None: {e}")
        milliseconds_value = None
        return milliseconds_value
    except Exception as e:
        # Show me what line the error occurred on
        logging.error(f"An error occurred tranforming ms to seconds. Returned None: {e}", exc_info=True)
        milliseconds_value = None
        return milliseconds_value
    
def transform_headers(headers):
    transformed_headers = []

    for header in headers:
        modified_header = header.replace(' ', '_').replace('(', '').replace(')', '')
        transformed_headers.append(modified_header.lower())

    return transformed_headers

# *Transform Statistics*
def transform_statistics(data, headers, headers_to_convert_to_seconds, headers_to_convert_to_int, statistics_type):

    # Create a list to store InfluxDB data points
    tranformed_data = []

    # Iterate through each list in data
    for row_data in data:

        # Process the rest of the columns
        for i, (header, value) in enumerate(zip(headers[0:], row_data[0:])):
        # Check if value is not None
            if value is not None:
                try:
                    # Is the value a date? 
                    if header == "state_since":
                        date_value = pytz.timezone('US/Central').localize(datetime.strptime(value, "%Y-%m-%d %H:%M:%S"))
                        cst_date_value = date_value + timedelta(hours=2)
                        row_data[i] = cst_date_value  

                        state_timer_value = calculate_state_timer(cst_date_value)
            
                        row_data.append(state_timer_value)

                    elif header in headers_to_convert_to_seconds:
                        seconds_value = transform_ms_to_seconds(value)
                        row_data[i] = seconds_value
                    elif header in headers_to_convert_to_int:
                        try:
                            row_data[i] = int(value)
                        except ValueError as e:
                            logging.error(f"Failed to convert {value} to integer. Returned None: {e}")
                            row_data[i] = None
                except Exception as e:
                    logging.error(f"Error with {header}:{value}: {e}")
        
            # End of if Value is not None
        # Done processing the single row in data
        
        # Add the Point to the list
        tranformed_data.append(row_data)

    # End of iterating through each list in 'data'
    logging.info(f"Number of {statistics_type} rows after transformation: {len(tranformed_data)}")
    return headers, tranformed_data

#  *Initialize Stats*
def stats_column_names_dictionary(statistics_type):

    column_names_dictionary = []
    headers_to_convert_to_seconds = []
    headers_to_convert_to_int = []
    table_name = ""

    if statistics_type == 'AgentState':
        column_names = ["Full Name", "Username", "State", "State Since", "Call Type", "Customer", "On Hold Duration", "Reason Duration", "Not Ready State Duration", "On Call State Duration"]
        column_names_dictionary = {"values": {"data": column_names}} 
        headers_to_convert_to_seconds = ["on_hold_duration", "reason_duration", "not_ready_state_duration", "on_call_state_duration"]
        table_name = "public.\"agentState\""
        # table_name = "dev.\"agentState\""
    elif statistics_type == 'AgentStatistics':
        column_names = ["Full Name", "Username", "Total Calls", "Avg Talk Time", "Avg Handle Time", "Avg Hold Time", "Avg Idle Time", "Avg Not Ready Time", "Avg Wrap Time", "Total Inbound Calls", "Total Manual Calls"]
        column_names_dictionary = {"values": {"data": column_names}}
        headers_to_convert_to_seconds = ["avg_talk_time", "avg_handle_time", "avg_hold_time", "avg_idle_time", "avg_not_ready_time", "avg_wrap_time"]
        table_name = "public.\"agentStatistics\""
        # table_name = "dev.\"agentStatistics\""
    elif statistics_type == 'ACDStatus':
        column_names = ["Skill Name", "Agents Logged In", "Agents Not Ready For Calls", "Agents On Call", "Agents Ready For Calls", "Agents Ready For VMs", "Calls In Queue", "Calls In Queue (Visual IVR)", "Current Longest Queue Time", "Longest Queue Time", "Queue Callbacks", "Total VMs", "VMs In Progress", "VMs In Queue"]
        column_names_dictionary = {"values": {"data": column_names}}
        headers_to_convert_to_seconds = ["longest_queue_time", "current_longest_queue_time"]
        headers_to_convert_to_int = ["agents_not_ready_for_calls", "agents_on_call", "agents_ready_for_calls", "agents_ready_for_vms", "calls_in_queue_visual_ivr", "queue_callbacks", "total_vms", "vms_in_progress", "vms_in_queue"]
        table_name = "public.\"acdStatus\""
        # table_name = "dev.\"acdStatus\""
    return column_names_dictionary, headers_to_convert_to_seconds, headers_to_convert_to_int, table_name
    
def get_stats(statistics_type):
    # Connect to Five9
    five9_client = connect_to_five9()

    # Get column names and headers to convert to seconds and headers to convert to int
    column_names_dictionary, headers_to_convert_to_seconds, headers_to_convert_to_int, table_name = stats_column_names_dictionary(statistics_type)
    
    # Get the statistics
    statistics = five9_client.supervisor.getStatistics(statisticType=statistics_type, columnNames=column_names_dictionary)

    # Get the headers and data
    original_headers = statistics['columns']['values']['data']
    headers = transform_headers(original_headers)
    if statistics_type == 'AgentState':
        headers.append('state_timer')
    data = [row['values']['data'] for row in statistics['rows']]
    logging.info(f"Number of {statistics_type} rows from F9: {len(data)}")
    # Return the headers and transformed data
    return headers, data, headers_to_convert_to_seconds, headers_to_convert_to_int, statistics_type, table_name

# *Load Data*
def load_statistics_to_postgres(headers, transformed_data, db_cursor, table_name):
    # Define the PostgreSQL connection URL
    
    insert_query = "INSERT INTO {} ({}) VALUES ({})".format(table_name,', '.join(['\"' + col + '\"' for col in headers]), ', '.join(['%s']*len(headers)))

    db_cursor.executemany(insert_query, transformed_data)
    conn.commit()
    # print(f'write to {table_name} in postgres complete')
    logging.info(f"Data Successfully Loaded to Postgres for {table_name}")

# *Delete Data*
def delete_data_from_postgres(db_cursor, table_name):
    try:

        # table_name = "public.\"agentState\""
        delete_query = f"DELETE FROM {table_name}"
        db_cursor.execute(delete_query)
        conn.commit()
        logging.info(f"Data Successfully Deleted from Postgres for {table_name}")
    except Exception as e:
        logging.error(f"An error occurred while deleting data from postgres: {e}")

# *Statistics*
def agent_state():

    agent_state_headers, agent_state_data, agent_state_headers_to_convert_to_seconds, agent_state_headers_to_convert_to_int, statistics_type, agent_state_table_name  = get_stats('AgentState')
    agent_state_headers, agent_state_data = transform_statistics(agent_state_data, agent_state_headers, agent_state_headers_to_convert_to_seconds, agent_state_headers_to_convert_to_int, statistics_type)
    delete_data_from_postgres(db_cursor, agent_state_table_name)
    load_statistics_to_postgres(agent_state_headers, agent_state_data, db_cursor, agent_state_table_name)
    

def agent_statistics():
    agent_statistics_headers, agent_statistics_data, agent_statistics_headers_to_convert_to_seconds, agent_statistics_headers_to_convert_to_int, statistics_type, agent_statistics_table_name  = get_stats('AgentStatistics')
    agent_statistics_headers, agent_statistics_data = transform_statistics(agent_statistics_data, agent_statistics_headers, agent_statistics_headers_to_convert_to_seconds, agent_statistics_headers_to_convert_to_int, statistics_type)
    delete_data_from_postgres(db_cursor, agent_statistics_table_name)
    load_statistics_to_postgres(agent_statistics_headers, agent_statistics_data, db_cursor, agent_statistics_table_name)
    

def acd_status():

    acd_status_headers, acd_status_data, acd_status_headers_to_convert_to_seconds, acd_status_headers_to_convert_to_int, statistics_type, acd_status_table_name  = get_stats('ACDStatus')
    acd_status_headers, acd_status_data = transform_statistics(acd_status_data, acd_status_headers, acd_status_headers_to_convert_to_seconds, acd_status_headers_to_convert_to_int, statistics_type)
    delete_data_from_postgres(db_cursor, acd_status_table_name)
    load_statistics_to_postgres(acd_status_headers, acd_status_data, db_cursor, acd_status_table_name)
    

def nty_notification(notification_message):
    requests.post("https://ntfy.sh/Nitor_Ops", 
        data=notification_message.encode(encoding='utf-8'))

# *--------------------- Main ---------------------*
# *ESTABLISH CONNECTIONS*
# Connect to the PostgreSQL database server
    
try:
    conn, db_cursor = connect_to_postgres()
    logging.info("Connected to the PostgreSQL database server")
except psycopg2.DatabaseError as error:
    logging.error(error)
    nty_notification(f"Program: Five9_ETL/stats.py \nAn error occurred while connecting to the PostgreSQL database server:\n {error}")
except Exception as e:
    logging.error(e)
    nty_notification(f"Program: Five9_ETL/stats.py \nAn unexpected error occurred while connecting to the PostgreSQL database server:\n {e}")


try:
    agent_state()
    agent_statistics()
    acd_status()
    # nty_notification("Five9_ETL stats.py has successfully completed")
except Exception as e:
    if 'maximum' in str(e):
        logging.error(e)
        nty_notification(f"agentDetail error. Max supervisors\n {e}")
        send_telegram_message(chat_id, message, bot_token)
        print(message)
    elif 'Value 68,400,000 of "ViewSettings.timeZone" is' in str(e):
        pass

    else:
        logging.error(e)
        nty_notification(f"Program: Five9_ETL/stats.py \nAn unexpected error occurred: {e}")
print('Done')