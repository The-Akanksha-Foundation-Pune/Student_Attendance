#!/usr/bin/env python3
"""
Standalone Daily SWPTM ETL Script
Includes database setup and all required functionality
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import logging
import mysql.connector
import configparser
import urllib3
import re

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    filename='swptm_daily.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize config parser
config = configparser.ConfigParser()
config.read('config.ini')

# Access the API details
api_url = config.get('api', 'swptm_url', fallback='https://akanksha.edustems.com/getStudentAttendanceSWPTM.htm')
api_key = config['api']['key']

# Access MySQL configuration
db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'port': config['mysql']['port'],
    'database': config['mysql']['database']
}

# Database table creation SQL for SWPTM data
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS swptm_attendance_data (
    id INT AUTO_INCREMENT PRIMARY KEY,
    school_name VARCHAR(255) NOT NULL,
    grade_name VARCHAR(10) NOT NULL,
    division_name VARCHAR(10) NOT NULL,
    course_name VARCHAR(255) NOT NULL,
    student_id VARCHAR(255) NOT NULL,
    student_name VARCHAR(255) NOT NULL,
    academic_year VARCHAR(10) NOT NULL,
    gender CHAR(10) NOT NULL,
    month VARCHAR(20) NOT NULL,
    date DATE NOT NULL,
    no_of_working_days INT NOT NULL,
    no_of_present_days INT NOT NULL,
    attendance_percentage VARCHAR(255) NOT NULL,
    timestamp DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    swptm_attendance_data_unique_key VARCHAR(500) UNIQUE
);
"""

def setup_database():
    """Create the swptm_attendance_data table if it doesn't exist"""
    try:
        # Connect to MySQL
        logging.info("Connecting to MySQL database...")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute the table creation
        logging.info("Creating swptm_attendance_data table if it doesn't exist...")
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        
        logging.info("Database table setup completed successfully!")
        
        # Verify table exists
        cursor.execute("SHOW TABLES LIKE 'swptm_attendance_data'")
        result = cursor.fetchone()
        if result:
            logging.info("Table verification successful!")
        else:
            logging.error("Table verification failed!")
            
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        raise
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            logging.info("Database connection closed.")

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
        "I": "1", "II": "2", "III": "3", "IV": "4", "V": "5",
        "VI": "6", "VII": "7", "VIII": "8", "IX": "9", "X": "10",
        "XI": "11", "XII": "12"
    }
    
    match = roman_to_number_pattern.search(value)
    if match:
        roman = match.group(1)
        if roman in roman_to_number:
            return f"GRADE {roman_to_number[roman]}"
    
    return value

def format_date_column(original_date):
    try:
        formatted_date = datetime.strptime(original_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return formatted_date
    except ValueError:
        logging.error(f"Invalid date format: {original_date}")
        return original_date

def clean_gender(value):
    if not value or not isinstance(value, str):
        return value
    
    original = value
    value = value.strip().lower()
    
    if value in ['m', 'male', 'boy', 'male']:
        return 'M'
    elif value in ['f', 'female', 'girl', 'female']:
        return 'F'
    else:
        logging.warning(f"Unknown gender value: {original}, keeping as is")
        return original

def extract_division(value):
    if not value or not isinstance(value, str):
        return value
    
    # Extract division from patterns like "A", "B", "1", "2", etc.
    division_match = re.search(r'[A-Z]|\d+', value.strip())
    if division_match:
        return division_match.group()
    
    return value

def trim_string(value):
    if isinstance(value, str):
        return value.strip()
    return value

def generate_swptm_unique_key(row, academic_year, month):
    """Generate unique key for SWPTM data"""
    return f"{academic_year}|{month}|{row['school_name']}|{row['grade_name']}|{row['student_id']}"

def get_academic_year(date_obj):
    """Determine academic year from date"""
    if date_obj.month < 6:
        return f"{date_obj.year - 1}-{date_obj.year}"
    else:
        return f"{date_obj.year}-{date_obj.year + 1}"

def get_month_name(date_obj):
    """Get month name from date object"""
    return date_obj.strftime('%B')

def fetch_data_from_api(academic_year, month):
    """Fetch data from SWPTM API"""
    try:
        url_with_params = f"{api_url}?api-key={api_key}&academic_year={academic_year}&month_name={month}"
        logging.info(f"Fetching data from API: {url_with_params}")
        
        response = requests.get(url_with_params, verify=False)
        response.raise_for_status()
        
        data = response.json()
        if data.get('status') and len(data.get('status', [])) > 0 and data['status'][0].get('message') == 'SUCCESS' and 'data' in data:
            df = pd.DataFrame(data['data'])
            logging.info(f"Successfully fetched {len(df)} records from API")
            return df
        else:
            logging.warning(f"API returned status: {data.get('status', 'unknown')}")
            return pd.DataFrame()
            
    except Exception as e:
        logging.error(f"Error fetching data from API: {e}")
        return pd.DataFrame()

def insert_data_to_mysql(df, stats):
    """Insert data into MySQL database"""
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO swptm_attendance_data 
        (school_name, grade_name, division_name, course_name, student_id, student_name, 
         academic_year, gender, month, date, no_of_working_days, no_of_present_days, 
         attendance_percentage, swptm_attendance_data_unique_key)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        no_of_working_days = VALUES(no_of_working_days),
        no_of_present_days = VALUES(no_of_present_days),
        attendance_percentage = VALUES(attendance_percentage),
        timestamp = CURRENT_TIMESTAMP
        """
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0
        
        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    row['school_name'], row['grade_name'], row['division_name'], 
                    row['course_name'], row['student_id'], row['student_name'],
                    row['academic_year'], row['gender'], row['month'], row['date'],
                    row['total_no_of_swptm'], row['present_swptm'], 
                    row['attendance_percentage'], row['swptm_attendance_data_unique_key']
                ))
                conn.commit()
                inserted_count += 1
            except mysql.connector.Error as err:
                if err.errno == 1062:  # Duplicate entry error
                    duplicate_count += 1
                    logging.warning(f"Duplicate entry skipped: {row['swptm_attendance_data_unique_key']}")
                else:
                    error_count += 1
                    logging.error(f"Database error for row {row['swptm_attendance_data_unique_key']}: {err}")
                    raise
        
        # Update statistics
        stats['new_records_inserted'] = inserted_count
        stats['duplicate_records_skipped'] = duplicate_count
        stats['error_records'] = error_count
        
        logging.info(f"Database insert completed: {inserted_count} inserted, {duplicate_count} duplicates, {error_count} errors")
        
    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        stats['error_records'] = len(df)
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def process_daily_data():
    """Process daily SWPTM data for current month"""
    logging.info("=== Starting Daily SWPTM ETL Process ===")
    start_time = datetime.now()
    
    # Setup database
    setup_database()
    
    # Get current date and determine academic year and month
    current_date = datetime.now()
    academic_year = get_academic_year(current_date)
    month_name = get_month_name(current_date)
    
    logging.info(f"Processing data for {month_name} {academic_year}")
    
    # Fetch data from API
    df = fetch_data_from_api(academic_year, month_name)
    
    if df.empty:
        logging.info(f"No data found for {month_name} {academic_year}")
        return
    
    # Clean and process data
    logging.info(f"Starting data cleaning for {len(df)} records...")
    df = df.map(trim_string)
    logging.info("✓ Trimmed whitespace from all fields")
    
    df['student_name'] = df['student_name'].apply(clean_student_name)
    logging.info("✓ Cleaned student names")
    
    df['grade_name'] = df['grade_name'].apply(convert_grade_name)
    logging.info("✓ Converted grade names")
    
    df['date'] = df['date'].apply(format_date_column)
    logging.info("✓ Formatted dates")
    
    df['gender'] = df['gender'].apply(clean_gender)
    logging.info("✓ Cleaned gender values")
    
    df['division_name'] = df['division_name'].apply(extract_division)
    logging.info("✓ Extracted division names")
    
    # Generate unique keys
    df['swptm_attendance_data_unique_key'] = df.apply(
        lambda row: generate_swptm_unique_key(row, academic_year, month_name), axis=1
    )
    
    # Insert data
    stats = {}
    insert_data_to_mysql(df, stats)
    
    # Final statistics
    end_time = datetime.now()
    processing_time = (end_time - start_time).total_seconds()
    
    logging.info("=== Daily SWPTM ETL Process Completed ===")
    logging.info(f"Processing time: {processing_time:.2f} seconds")
    logging.info(f"Records fetched: {len(df)}")
    logging.info(f"Records inserted: {stats.get('new_records_inserted', 0)}")
    logging.info(f"Duplicates skipped: {stats.get('duplicate_records_skipped', 0)}")
    logging.info(f"Errors: {stats.get('error_records', 0)}")

if __name__ == "__main__":
    process_daily_data()
