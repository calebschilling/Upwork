# Standard library imports
import json
import logging
import os
import re
import string
from configparser import ConfigParser
from datetime import datetime

# Related third party imports
import psycopg2
import requests

# Basic Configurations
# Logging
LOG_FILE_PATH = "/home/etl_user/logs/wufoo.log"
# LOG_FILE_PATH = "wufoo.log"

# Configure logging
logging.basicConfig(
    filename=LOG_FILE_PATH,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s]: %(message)s",
    datefmt="%b. %d, %Y %I:%M:%S %p",  # Custom date and time format
)

# Config file
config = ConfigParser()
config_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.ini")
config.read(config_path)

# Field mapping
# Build of mapping of report names and their UOCs
uoc_dictionary = {
    "Utility Company STOP Service Form": "Utility Company",
    "Utility Company Start Service Form": "Utility Company",
    "St. Joseph's Glen UOC Stop Service Form": "St. Joseph's Glen",
    "St. Joseph's Glen UOC Start Service Form": "St. Joseph's Glen",
    "Sebastian Lake UOC Stop Service Form": "Sebastian Lake",
    "Sebastian Lake UOC Start Service Form": "Sebastian Lake",
    "Red Bird UOC Stop Service Form": "Red Bird",
    "Red Bird UOC Start Service Form": "Red Bird",
    "Raccoon Creek UOC Stop Service Form": "Raccoon Creek",
    "Raccoon Creek UOC Start Service Form": "Raccoon Creek",
    "Osage UOC Stop Service Form": "Osage",
    "Osage UOC Start Service Form": "Osage",
    "Oak Hill UOC Stop Service Form Copy": "Oak Hill",
    "Oak Hill UOC Start Service Form": "Oak Hill",
    "Magnolia Water UOC Stop Service Form": "Magnolia Water",
    "Magnolia Water UOC Start Service Form": "Magnolia Water",
    "Limestone UOC Stop Service Form": "Limestone Water",
    "Limestone UOC Start Service Form Copy": "Limestone Water",
    "Indian Hills UOC Stop Service Form": "Indian Hills",
    "Indian Hills UOC Start Service Form": "Indian Hills",
    "Hillcrest UOC Stop Service Form": "Hillcrest",
    "Hillcrest UOC Start Service Form": "Hillcrest",
    "Hayden's Place UOC Stop Service Form": "Hayden's Place",
    "Hayden's Place UOC Start Service Form": "Hayden's Place",
    "Great River UOC Start Service Form": "Great River",
    "Great River UOC Stop Service Form": "Great River",
    "Flushing Meadows UOC Stop Service Form": "Flushing Meadows",
    "Flushing Meadows UOC Start Service Form": "Flushing Meadows",
    "Elm Hills UOC Stop Service Form": "Elm Hills",
    "Elm Hills UOC Start Service Form": "Elm Hills",
    "Eagle Ridge UOC Stop Service Form": "Eagle Ridge",
    "Eagle Ridge UOC Start Service Form": "Eagle Ridge",
    "CSWR TX UOC Stop Service Form": "CSWR-Texas",
    "CSWR TX UOC Start Service Form": "CSWR-Texas",
    "CSWR SC UOC Stop Service Form": "CSWR-South Carolina",
    "CSWR SC UOC Start Service Form": "CSWR-South Carolina",
    "CSWR Florida UOC Stop Service Form": "CSWR-Florida",
    "CSWR Florida UOC Start Service Form": "CSWR-Florida",
    "CSWR California UOC Stop Service Form": "CSWR-California",
    "CSWR California UOC Start Service Form": "CSWR-California",
    "Contact Customer Support": "Contact Customer Support",
    "Contact CSWR": "Contact CSWR",
    "Conflluence Rivers UOC Stop Service Form": "Confluence Rivers",
    "Conflluence Rivers UOC Start Service Form": "Confluence Rivers",
    "Cactus State UOC Stop Service Form": "Cactus State",
    "Cactus State UOC Start Service Form": "Cactus State",
    "Bluegrass UOC Stop Service Form": "Bluegrass",
    "Bluegrass UOC Start Service Form": "Bluegrass",
}

# Build mapping of service fields
field_mapping = {
    "Field376": "account_status",
    "Field377": "WAITING_on_Info",
    "Field379": "Duplicate",
    "Field378": "see_comments",
    "Field380": "Billing_to_work",
    "Field381": "Stacie",
    "Field792": "Start_service_at_Residential_or_Commercial_Property",
    "Field794": "existing_property_or_new_construction",
    "Field1": "First_Name",
    "Field2": "Last_Name",
    "Field3": "Phone_Number",
    "Field796": "Mobile_Phone_Number",
    "Field4": "Email",
    "Field585": "Preferred_communication",
    "Field799": "Preferred_communication_other",
    "Field802": "Keep_me_updated_with_latest_CSWR_news_other",
    "Field797": "Keep_me_updated_with_latest_CSWR_news",
    "Field5": "Starting_Service_Address",
    "Field6": "Address_Line_2",
    "Field7": "City",
    "Field8": "State_Province_Region",
    "Field9": "Postal_Zip_Code",
    "Field10": "Country",
    "Field217": "Date_to_Start_Stop_Service",
    "Field11": "rent_or_own_property",
    "Field215": "Landlord_Name",
    "Field216": "Landlord_Last",
    "Field323": "Landlord_Address",
    "Field324": "Landlord_Address_Line_2",
    "Field325": "Landlord_City",
    "Field326": "Landlord_State_Province_Region",
    "Field327": "Landlord_Postal_Zip_Code",
    "Field328": "Landlord_Country",
    "Field330": "Landlord_Phone_Number",
    "Field329": "Landlord_Email",
    "Field360": "Billing_address_same_as_service_address",
    "Field361": "Billing_Address",
    "Field362": "Billing_Address_Line_2",
    "Field363": "Billing_City",
    "Field364": "Billing_State_Province_Region",
    "Field365": "Billing_Postal_Zip_Code",
    "Field366": "Billing_Country",
    "Field340": "Comments_or_Additional_Information",
    "Field801": "Home_Phone_Number",
    "EntryId": "Entry_Id",
    "DateCreated": "Date_Created",
    "CreatedBy": "Created_By",
    "DateUpdated": "Last_Updated",
    "UpdatedBy": "Updated_By",
    "IP": "IP_Address",
    "LastPage": "Last_Page_Accessed",
    "CompleteSubmission": "Completion_Status",
    "Field484": "choose_the_uoc_with_whom_you_would_like_to_stop_services",
    "Field477": "choose_the_uoc_with_whom_you_would_like_to_start_services",
}

def getCsrFormStatus(form, uoc_name):
    list_of_columns = [
        'account_status',
        'WAITING_on_Info',
        'Duplicate',
        'see_comments',
        'Billing_to_work'
    ]

    list_of_columns_with_stacie = [
        'account_status',
        'WAITING_on_Info',
        'Duplicate',
        'see_comments',
        'Billing_to_work',
        'Stacie'
    ]

    uocs_to_omit = ['Contact Customer Support', 'Contact CSWR', 'Utility Company']

    value = None

    # if uoc_name is not in uocs_to_omit then do this, else pass this form
    if uoc_name not in uocs_to_omit:
        
        # loop through the dictionary keys of form and see if the values are not ''
        for key in list_of_columns:
            try:
                if form[key] != '':
                
                    value = form[key]
                    # make it all lower case
                    value = value.lower()
                    # strip any whitespace
                    value = value.strip()
                
            except Exception as e:
                value = None

            try:
                # loop through the form with the keys in the list_of_status_columns and check if they are all empty. If they are. make the value 'needs to be worked'
                if all(form[key] == '' for key in list_of_columns_with_stacie):
                    value = 'needs to be worked'
            except Exception as e:
                value = 'needs to be worked'
    else:
        value = 'historical form only'
        
    return value
            
# Functions
# Connect to Postgres
def connect_to_postgres():  # Connect to the PostgreSQL database server: return conn, db_cursor
    # Establish a connection to the PostgreSQL database
    conn = psycopg2.connect(
        dbname=config.get("Postgres", "db_name"),
        user=config.get("Postgres", "db_username"),
        password=config.get("Postgres", "db_password"),
        host=config.get("Postgres", "db_host"),
        port=config.get("Postgres", "db_port"),
    )

    # Create a cursor object
    db_cursor = conn.cursor()
    return conn, db_cursor

# Build report entries URL function
def report_entries_url(report_hash):
    response = requests.get(
        base_url + "reports/" + report_hash + "/entries.json?system=true",
        auth=(username, password),
    )
    return response.json()

# Is the table empty?
def is_query_empty_postgres():
    conn, db_cursor = connect_to_postgres()
    db_cursor.execute("select * from wufoo.reports_backup limit 1")
    results = db_cursor.fetchall()
    conn.close()
    if len(results) == 0:
        return True
    else:
        return False

def get_previous_forms(report_hash):
    conn, db_cursor = connect_to_postgres()
    previous_forms_query = f"select * from wufoo.reports_backup where report_hash = '{report_hash}' ORDER BY entry_id::integer ASC"
    db_cursor.execute(previous_forms_query)
    previous_query_results = db_cursor.fetchall()

    column_names_query = f"SELECT column_name FROM information_schema.columns WHERE table_name = 'reports_backup' ORDER BY ordinal_position ASC"
    db_cursor.execute(column_names_query)
    column_names_results = db_cursor.fetchall()

    conn.close()
    return previous_query_results, column_names_results

# STEP 5
def update_previous_forms(current_report_forms, report_hash):
    # get the previous forms from the database and column names 
    # to pair the results from previous forms with the current report forms
    # 
    previous_forms, column_names = get_previous_forms(
        report_hash
    )  # get form from postgres

    # use the column names to pair the results from previous forms
    column_names = [column[0] for column in column_names]
    previous_forms = [dict(zip(column_names, form)) for form in previous_forms]

    

    for previous_form in previous_forms:
        previous_entry_id = str(previous_form['entry_id'])
        # previous_entry_id = previous_form["entry_id"]  
        # get entry id where key is entry_id
        if report_hash == previous_form["report_hash"]:
            current_form = next(
                (
                    form
                    for form in current_report_forms
                    if form["EntryId"] == previous_entry_id
                ),
                None,
            )  
  
            if current_form:
                try:
                    # transform the current_from keys into the field_mapping values with the keys
                    current_form = {
                        field_mapping[key]: value
                        for key, value in current_form.items()
                        if key in field_mapping
                    }
                    # make all keys in current_form lowercase
                    uoc_name = previous_form['uoc_name']
                    value = getCsrFormStatus(current_form,uoc_name)
                    current_form["csr_form_status"] = value
                    current_form = {key.lower(): value for key, value in current_form.items()}
                    
                    update_database_if_changed(current_form, previous_form, report_hash)
                except Exception as e:
                    logging.error(f"Error updating database: {e}", exc_info=True)
                    print(f"Error updating database: {e}")

    print("Done looking for updates for report_hash: ", report_hash)

def update_database_if_changed(current_form, previous_form, report_hash):
    changes_detected = False
    updated_values = {}

    # # transform the current_from keys into the field_mapping values with the keys
    # current_form = {
    #     field_mapping[key]: value
    #     for key, value in current_form.items()
    #     if key in field_mapping
    # }
    # # make all keys in current_form lowercase
    # current_form = {key.lower(): value for key, value in current_form.items()}
    
    # convert entry_id to int
    try:
        current_form["entry_id"] = int(current_form["entry_id"])
    except Exception as e:
        print("ERROR CONVERTING 'entry_id' TO INT")
        print(e)

    # convert all date strings to datetime objects
    current_form["date_created"] = datetime.strptime(
        current_form["date_created"], "%Y-%m-%d %H:%M:%S"
    )
    try:
        current_form["last_updated"] = datetime.strptime(
            current_form["last_updated"], "%Y-%m-%d %H:%M:%S"
        )
    except Exception as e:
        current_form["last_updated"] = None

    try: 
        current_form["date_to_start_stop_service"] = datetime.strptime(
            current_form["date_to_start_stop_service"], "%Y-%m-%d"
        ).date()
    except Exception as e:
        current_form["date_to_start_stop_service"] = None


    for field, value in current_form.items():

        if field in previous_form and previous_form[field] != value:
            updated_values[field] = value
            changes_detected = True
            

    if changes_detected:
        update_database(report_hash, previous_form["entry_id"], updated_values)
        print_update_message(report_hash, previous_form["entry_id"], updated_values)
        # flush the updated_values
        updated_values = {}
        # print("Database updated. Dictionary Flushed.")

def update_database(report_hash, entry_id, updated_values):
    conn, db_cursor = connect_to_postgres()
    update_query = """
        UPDATE wufoo.reports_backup
        SET
    """
    set_clause = ""
    for field, value in updated_values.items():
        set_clause += f"{field} = %s,"
    set_clause = set_clause.rstrip(",")
    update_query += (
        set_clause
        + """
        WHERE
            entry_id = %s
            AND report_hash = %s
    """
    )
    values = list(updated_values.values())
    values.extend([entry_id, report_hash])
    try:
        db_cursor.execute(update_query, tuple(values))
    except ValueError as ve:
        print(ve)
        logging.error(f"Error updating database: {ve}", exc_info=True)
        for field, value in updated_values.items():
            # print(f" - {field}: {value}")
            # remove exif
             
            index_of_exif = value.index("Exif")
            cleaned_data = value[:index_of_exif]
            value = cleaned_data 
            # replace the old value with new
            updated_values[field] = value
            # update the database
            db_cursor.execute(update_query, tuple(values))

    conn.commit()
    conn.close()

def print_update_message(report_hash, entry_id, updated_values):
    print(f"Database updated for report_hash: {report_hash}, entry_id: {entry_id}")
    for field, value in updated_values.items():
        print(f" - {field}: {value}")

def isFormEntryEmpty(form):
    
    isEmpty = False

    try:
        if form['First_Name'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    try:
        if form['Last_Name'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    try:
        if form['Phone_Number'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    try:
        if form['Email'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    try:
        if form['Starting_Service_Address'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    try:
        if form['Phone_Number'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    try:
        if form['Mobile_Phone_Number'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    try:
        if form['Home_Phone_Number'] == '':
            isEmpty = True
        else:
            isEmpty = False
    except Exception as e:
        pass

    
    return isEmpty



# Get the max entry id's from all reports
def get_max_entry_id_postgres(report_hash):
    conn, db_cursor = connect_to_postgres()
    db_cursor.execute(
        f"select * from wufoo.max_entry_id_for_reports where report_hash = '{report_hash}'"
    )
    max_entry_id_results = db_cursor.fetchall()
    conn.close()
    return max_entry_id_results

# Used when transforming forms to know if we should add that entry or not
def foundEntryIdandHash(entry_id, report_hash):
    conn, db_cursor = connect_to_postgres()
    db_cursor.execute(
        f"select * from wufoo.reports_backup where entry_id = '{entry_id}' and report_hash = '{report_hash}'"
    )
    entry_id_results = db_cursor.fetchall()
    conn.close()

    # if empty return False else return True
    if len(entry_id_results) == 0:
        return False
    else:
        return True
    
# Insert data into database function
def insert_into_postgres(transformed_data):
    conn, db_cursor = connect_to_postgres()
    headers = list(transformed_data.keys())

    # Prepare placeholders for the values
    value_placeholders = ", ".join(["%s" for _ in headers])

    # Get values from transformed_data
    values = list(transformed_data.values())

    try:
        # Insert data into database
        insert_query = "INSERT INTO wufoo.reports_backup ({}) VALUES ({})".format(
            ",".join(headers), value_placeholders
        )
        db_cursor.execute(insert_query, values)
        conn.commit()
        conn.close()
        logging.info("Data inserted into database")
        print("Data inserted into database")  # testing purposes
    except ValueError as ve:
        # Clean the value where header index is 'Comments_or_Additional_Informationn
        index_of_comments = headers.index("Comments_or_Additional_Information")

        # clean the value where index is 'comments_or_additional'
        line_to_clean = values[index_of_comments]

        # clean non character values from the string
        # cleaned_text = re.sub(r'[^a-zA-Z0-9\s]', '', line_to_clean)

        if "Exif" in line_to_clean:
            # Remove the binary data
            index_of_exif = line_to_clean.index("Exif")
            cleaned_data = line_to_clean[:index_of_exif]

        else:
            cleaned_text = re.sub(r"[^a-zA-Z0-9\s]", "", line_to_clean)

        values[index_of_comments] = cleaned_data

        # Insert data into database
        insert_query = "INSERT INTO wufoo.reports_backup ({}) VALUES ({})".format(
            ",".join(headers), value_placeholders
        )
        db_cursor.execute(insert_query, values)
        conn.commit()
        conn.close()

    except Exception as e:
        print(e)
        logging.error(f"Data not inserted into database {e}", exc_info=True)
        logging.error(f"{insert_query}")
        conn.close()
        print("Data not inserted into database")
        # send_ntfy_notification(f"Wufoo data not inserted into database: {e}")

def getEntryIdsFromReportHash(report_hash):
    conn, db_cursor = connect_to_postgres()
    db_cursor.execute(
        f"select entry_id from wufoo.reports_backup where report_hash = '{report_hash}'"
    )
    entry_id_results = db_cursor.fetchall()
    # turn into a list of integers
    entry_id_results = [int(entry_id[0]) for entry_id in entry_id_results]
    conn.close()
    return entry_id_results

# STEP 4
# Create function to transform data
def transform_keys(report_forms, start_or_stop, first_run, report_hash):
    report_forms = report_forms["Entries"]
    # report_forms["Uoc_name"] = uoc_dictionary[report]
    try:
        # STEP 5 Check for updates
        if not first_run:
            update_previous_forms(report_forms, report_hash)
    except Exception as e:
        send_ntfy_notification(f"Error updating previous year of forms: {e}")


    # coampare_and_update(report_hash, report_forms, start_or_stop)
    entry_ids_for_this_report_hash = getEntryIdsFromReportHash(report_hash)

    # Loop through each form in report_forms
    for form in report_forms:
        if int(form["EntryId"]) in entry_ids_for_this_report_hash:
            pass
        
        else:
            transformed_data = {}  # Reinitialize the dictionary for each iteration
            for key, value in form.items():
                # Check if the key is in start_service_mapping
                if key in field_mapping:
                    # Replace key with the mapped value
                    transformed_key = field_mapping[key]
                    transformed_data[transformed_key] = value
                else:
                    # Keep other keys unchanged
                    transformed_data[key] = value

            isFormEmpty = isFormEntryEmpty(transformed_data)
            if isFormEmpty:
                pass
            else:
                # Add additional data to transformed_data
                transformed_data["Report_Name"] = report
                transformed_data["Uoc_name"] = uoc_dictionary[report]
                transformed_data["Report_Hash"] = report_names_and_hashes[report]
                transformed_data["Service_type"] = start_or_stop

                # Convert date_created to datetime object
                transformed_data["Date_Created"] = datetime.strptime(
                    transformed_data["Date_Created"], "%Y-%m-%d %H:%M:%S"
                )
                try:
                    # Convert last_updated to datetime object
                    transformed_data["Last_Updated"] = datetime.strptime(
                        transformed_data["Last_Updated"], "%Y-%m-%d %H:%M:%S"
                    )
                except Exception as e:
                    # Set last_updated to None if it can't be converted
                    transformed_data["Last_Updated"] = None

                try:
                    # Convert Field217 to a date object
                    transformed_data["Date_to_Start_Stop_Service"] = datetime.strptime(
                        transformed_data["Date_to_Start_Stop_Service"], "%Y-%m-%d"
                    )
                except Exception as e:
                    # Set Date_to_Start_Stop_Service to None if it can't be converted
                    transformed_data["Date_to_Start_Stop_Service"] = None

                # Check if the form is a CSR form           
                csr_form_status = getCsrFormStatus(transformed_data, transformed_data["Uoc_name"])
                transformed_data["csr_form_status"] = csr_form_status

                # 
                # Insert into database
                insert_into_postgres(transformed_data)

    print('Done in comparing and updating')

def send_ntfy_notification(notification):
    # Send ntfy notification that script is starting
    requests.post(
        "https://ntfy.sh/SchillingServer", data=notification.encode(encoding="utf-8")
    )

# 
# ----------------- START -----------------------
# 

# STEP 1
# Connect to Wufoo API and get all reports
base_url = config.get("Wufoo", "base_url")
username = config.get("Wufoo", "username")
password = config.get("Wufoo", "password")
response = requests.get(base_url + "reports.json", auth=(username, password))
all_reports_respone = response.json()
print(f"All {len(all_reports_respone['Reports'])} reports gathered from Wufoo API")

# STEP 2
# Create dictionary of report names and hashes to walk through
report_names_and_hashes = {}
for report in all_reports_respone["Reports"]:
    report_names_and_hashes[report["Name"]] = report["Hash"]

# Create a counter to keep track of the number of reports
counter = 0

# Get the max entry id's from all reports
first_run = is_query_empty_postgres()

# STEP 3
# Loop through each report from report_names_and_hashes and get the report entries
try:
    for report in report_names_and_hashes:
        if "Start" in report:
            logging.info(f"Working {report} report")
            print(f"Working {report} report")
            start_hash = report_names_and_hashes[report]
            start_report_forms = report_entries_url(start_hash)

            transformed_data = {}
            transform_keys(start_report_forms, "start", first_run, start_hash)
            print(f"{report} done")
            counter += 1

        else:
            logging.info(f"Working {report} report")
            print(f"Working {report} report")
            stop_hash = report_names_and_hashes[report]
            stop_report_forms = report_entries_url(stop_hash)

            transformed_data = {}
            transform_keys(stop_report_forms, "stop", first_run, stop_hash)
            print(f"{report} done")
            counter += 1
    print("Complete!")
    print("Total number of reports: ", counter)

    # Send ntfy notification that script is complete
    # send_ntfy_notification('WuFoo script complete')

except Exception as e:

    logging.error(f"Error getting report entries: {e}", exc_info=True)
    print(f"Error getting report entries: {e}")

    # send_ntfy_notification(f"WuFoo script failed: \n {e}")
