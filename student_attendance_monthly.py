import requests
import re
import pandas as pd
from datetime import datetime
import logging
import mysql.connector
import configparser

# Configure logging to track the process
logging.basicConfig(
    filename='student_attendance_monthly.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize config parser
config = configparser.ConfigParser()

# Read the configuration file
config.read('config.ini')

# Access the API details
api_url = config['api']['url']
api_key = config['api']['key']

# Access MySQL configuration
db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'port': config['mysql']['port'],
    'database': config['mysql']['database']
}

# Utility functions
def clean_student_name(value):
    return re.sub(r'\s+', " ", value).strip().title()

def convert_grade_name(value):
    if value == "Jr.KG":
        return "JR.KG"
    elif value == "Sr.KG":
        return "SR.KG"
    
    roman_to_number_pattern = re.compile(r"GRADE (\w+)")
    roman_to_number = {
        "I": "1", "II": "2", "III": "3", "IV": "4",
        "V": "5", "VI": "6", "VII": "7", "VIII": "8",
        "IX": "9", "X": "10"
    }
    match = roman_to_number_pattern.match(value)
    
    if match:
        roman_numeral = match.group(1)
        return f"GRADE {roman_to_number.get(roman_numeral, roman_numeral)}"
    return value

def format_date_column(original_date):
    try:
        formatted_date = datetime.strptime(original_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return formatted_date
    except ValueError:
        logging.error(f"Invalid date format: {original_date}")
        return original_date

def clean_gender(value):
    if not value:
        return None
    value = value.strip().upper()
    if value == "MALE":
        return "M"
    elif value == "FEMALE":
        return "F"
    return value

def extract_division(value):
    match = re.search(r'[A-Za-z]+', value)
    if match:
        return match.group(0)
    return value

def fill_na(df):
    return df.fillna("NA")

def fetch_unique_keys_from_mysql():
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = """
        SELECT student_attendance_data_unique_key FROM student_attendance_data
        """
        cursor.execute(query)
        result = cursor.fetchall()
        unique_keys = {item[0] for item in result}  # Using set to ensure uniqueness
        return unique_keys
    except mysql.connector.Error as err:
        logging.error(f"Database error while fetching unique keys: {err}")
        return set()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def insert_data_to_mysql(df):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO student_attendance_data (
            school_name, grade_name, division_name, course_name, student_id, student_name, academic_year,
            gender, month, date, no_of_working_days, no_of_present_days, attendance_percentage, student_attendance_data_unique_key
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        inserted_count = 0
        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    row['school_name'], row['grade_name'], row['division_name'], row['course_name'],
                    row['student_id'], row['student_name'], row['academic_year'], row['gender'],
                    row['month'], row['date'], row['no_of_working_days'], row['no_of_present_days'],
                    row['attendance_percentage'], row['student_attendance_data_unique_key']
                ))
                conn.commit()
                inserted_count += 1
            except mysql.connector.Error as err:
                if err.errno == 1062:  # Duplicate unique key
                    logging.error(f"Database error: {err}")
                else:
                    logging.error(f"Database error: {err}")

        if inserted_count > 0:
            logging.info(f"Inserted {inserted_count} records into the database.")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def fetch_data_from_api_and_process():
    today = datetime.now()
    current_year = today.year
    academic_year = f"{current_year-1}-{current_year}" if today.month < 5 else f"{current_year}-{current_year+1}"
    month_name = today.strftime('%B')

    params = {
        'api-key': api_key,
        'academic_year': academic_year,
        'month_name': month_name
    }

    url_with_params = f"{api_url}?api-key={params['api-key']}&academic_year={academic_year}&month_name={month_name}"
    logging.info(f"Requesting URL: {url_with_params}")

    response = requests.get(url_with_params)

    if response.status_code == 200:
        json_response = response.json()
        data = json_response.get('data', [])

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

            # Create unique keys for the API data
            df['student_attendance_data_unique_key'] = df.apply(
                lambda row: f"{row['academic_year']}|{row['month']}|{row['school_name']}|{row['grade_name']}|{row['student_id']}",
                axis=1
            )

            unique_keys_from_mysql = fetch_unique_keys_from_mysql()

            # Filter out rows that already exist in the database
            new_data_df = df[~df['student_attendance_data_unique_key'].isin(unique_keys_from_mysql)]

            if not new_data_df.empty:
                insert_data_to_mysql(new_data_df)

            logging.info(f"Data extracted for {academic_year}, {month_name}")
        else:
            logging.warning(f"No data available for {academic_year}, {month_name}")
    else:
        logging.error(f"Failed to retrieve data - Status code: {response.status_code}")

def main():
    fetch_data_from_api_and_process()

if __name__ == "__main__":
    main()
