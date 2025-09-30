import requests
import pandas as pd
import logging
from logging_config import setup_logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from database import fetch_unique_keys_from_mysql, insert_data_to_mysql, get_schools_with_data
from utils import clean_student_name, convert_grade_name, format_date_column, clean_gender, extract_division, fill_na, trim_dataframe, deduplicate_by_studentid
from configparser import ConfigParser
from school_config import SCHOOLS_ADMIN
from email_utils import send_email
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

setup_logging()

# Initialize config parser
config = ConfigParser()
config.read('config.ini')

# Access the API details
api_url = config['api']['url']
api_key = config['api']['key']

def fetch_data_from_api_and_process():
    today = datetime.now()
    previous_month_date = today - relativedelta(months=1)
    previous_month = previous_month_date.month
    previous_year = previous_month_date.year

    # Determine academic year
    academic_year = f"{previous_year-1}-{previous_year}" if previous_month < 5 else f"{previous_year}-{previous_year+1}"
    month_name = previous_month_date.strftime("%B")

    params = {
        'api-key': api_key,
        'academic_year': academic_year,
        'month_name': month_name
    }

    url_with_params = f"{api_url}?api-key={params['api-key']}&academic_year={academic_year}&month_name={month_name}"
    response = requests.get(url_with_params, verify=False)

    if response.status_code == 200:
        json_response = response.json()
        data = json_response.get('data', [])
        logging.info("Data fetched from API successfully.")

        if isinstance(data, list) and data:
            df = pd.DataFrame(data)
            df['student_name'] = df['student_name'].apply(clean_student_name)
            df['grade_name'] = df['grade_name'].apply(convert_grade_name)
            df['date'] = df['date'].apply(format_date_column)
            df['gender'] = df['gender'].apply(clean_gender)
            df['division_name'] = df['division_name'].apply(extract_division)
            df['academic_year'] = academic_year
            df['month'] = month_name
            df = fill_na(df)
            df = trim_dataframe(df)

            # Create unique keys for the API data BEFORE deduplication
            df['student_attendance_data_unique_key'] = df.apply(
                lambda row: f"{row['academic_year']}|{row['month']}|{row['school_name']}|{row['grade_name']}|{row['student_id']}",
                axis=1
            )

            # Get existing unique keys from database
            unique_keys_from_mysql = fetch_unique_keys_from_mysql()

            # Filter out rows that already exist in the database FIRST
            new_data_df = df[~df['student_attendance_data_unique_key'].isin(unique_keys_from_mysql)]

            # Then deduplicate the new data only
            if not new_data_df.empty:
                new_data_df = deduplicate_by_studentid(new_data_df)

            if not new_data_df.empty:
                insert_data_to_mysql(new_data_df)
                logging.info(f"New data inserted for {month_name} {academic_year}.")
            else:
                logging.info(f"No new data to insert for {month_name} {academic_year}.")
        else:
            logging.info(f"No data available for {academic_year}, {month_name}")
    else:
        logging.error(f"Failed to retrieve data - Status code: {response.status_code}")
        
def send_reminders_if_no_data():
    today = datetime.now()
    previous_month_date = today - relativedelta(months=1)
    previous_month = previous_month_date.month
    previous_year = previous_month_date.year
    month_name = previous_month_date.strftime("%B")

    # Determine academic year
    academic_year = f"{previous_year-1}-{previous_year}" if previous_month < 5 else f"{previous_year}-{previous_year+1}"

    # Fetch schools with data from DB
    schools_with_data = get_schools_with_data(academic_year, month_name)  # Returns list of school names

    for school_name, admin_email in SCHOOLS_ADMIN.items():
        if school_name not in schools_with_data:
            subject = f"Reminder: Submit Attendance Data for {month_name} {academic_year}"
            body = (
                f"Dear Admin,\n\n"
                f"This is a gentle reminder to submit the attendance data for **{school_name}** "
                f"for the month of {month_name} ({academic_year}).\n\n"
                f"Thank you."
            )
            send_email(to=admin_email, subject=subject, body=body)
            logging.info(f"Reminder email sent to {admin_email} for {school_name}")
