#!/usr/bin/env python3
"""
Standalone Student Attendance Monthly ETL Script
Includes database setup and all required functionality
"""

import requests
import re
import pandas as pd
from datetime import datetime
import logging
import mysql.connector
import configparser
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging to track the process
logging.basicConfig(
    filename='student_attendance_monthly.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Initialize config parser
config = configparser.ConfigParser()
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

# Database table creation SQL
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS student_attendance_data (
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
    student_attendance_data_unique_key VARCHAR(500) UNIQUE
);
"""

def setup_database():
    """Create the student_attendance_data table if it doesn't exist"""
    try:
        # Connect to MySQL
        logging.info("Connecting to MySQL database...")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Execute the table creation
        logging.info("Creating student_attendance_data table if it doesn't exist...")
        cursor.execute(CREATE_TABLE_SQL)
        conn.commit()
        
        logging.info("Database table setup completed successfully!")
        
        # Verify table exists
        cursor.execute("SHOW TABLES LIKE 'student_attendance_data'")
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
    
    male_values = {"male", "m", "man", "boy", "masculine"}
    female_values = {"female", "f", "woman", "girl", "feminine"}
    
    if value in male_values:
        return "M"
    elif value in female_values:
        return "F"
    
    logging.warning(f"Unrecognized gender value: {original}")
    return original

def extract_division(value):
    match = re.search(r'[A-Za-z]+', value)
    if match:
        return match.group(0)
    return value

def fill_na(df):
    return df.fillna("NA")

def trim_dataframe(df):
    return df.map(lambda x: x.strip() if isinstance(x, str) else x)

def deduplicate_by_studentid(df):
    # Define columns to check for duplicates
    subset_cols = ['student_id', 'student_name', 'gender', 'school_name']
    
    # Identify potential duplicates
    duplicated_mask = df.duplicated(subset=subset_cols, keep=False)
    df_duplicates = df[duplicated_mask].copy()
    total_duplicates = df_duplicates.shape[0]

    if total_duplicates > 0:
        logging.info(f"[Deduplication] Found {total_duplicates} potential duplicate rows based on {subset_cols}.")
    
    # Sort so highest attendance comes first
    df_duplicates.sort_values(
        by=subset_cols + ['attendance_percentage'],
        ascending=[True, True, True, True, False],
        inplace=True
    )

    # Keep one with highest attendance
    df_deduped = df_duplicates.drop_duplicates(subset=subset_cols, keep='first')

    # Identify dropped rows for logging
    df_dropped = pd.concat([df_duplicates, df_deduped]).drop_duplicates(keep=False)

    # Log each group and the kept/dropped rows
    grouped = df_duplicates.groupby(subset_cols)
    for key, group in grouped:
        logging.info(f"\n--- Duplicate Group: {key} ---")
        kept_rows = df_deduped[df_deduped[subset_cols].apply(tuple, axis=1) == key]
        logging.info(f"Kept:\n{kept_rows}")
        dropped_rows = df_dropped[df_dropped[subset_cols].apply(tuple, axis=1) == key] if not df_dropped.empty else pd.DataFrame()
        if not dropped_rows.empty:
            logging.info(f"Dropped:\n{dropped_rows}")

    # Combine deduplicated and unique data
    df_non_duplicates = df[~duplicated_mask]
    final_df = pd.concat([df_non_duplicates, df_deduped], ignore_index=True)

    logging.info(f"[Deduplication] Dropped {df_dropped.shape[0]} row(s). Kept {df_deduped.shape[0]} unique entries.")
    
    return final_df

def fetch_unique_keys_from_mysql():
    logging.info("Connecting to MySQL database.")
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        query = """
        SELECT student_attendance_data_unique_key FROM student_attendance_data
        """
        cursor.execute(query)
        result = cursor.fetchall()
        unique_keys = {item[0] for item in result}  # Using set to ensure uniqueness
        logging.info("Successfully connected to MySQL.")
        return unique_keys
    except mysql.connector.Error as err:
        logging.error(f"Database error while fetching unique keys: {err}")
        return set()
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def insert_data_to_mysql(df, stats):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO student_attendance_data (
        school_name, grade_name, division_name, course_name, student_id, student_name, academic_year, gender, month, date, no_of_working_days, no_of_present_days, attendance_percentage,
        student_attendance_data_unique_key) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        
        inserted_count = 0
        duplicate_count = 0
        error_count = 0

        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    row['school_name'], row['grade_name'], row['division_name'], row['course_name'],
                    row['student_id'], row['student_name'], row['academic_year'], row['gender'], row['month'], row['date'], row['no_of_working_days'], row['no_of_present_days'],
                    row['attendance_percentage'], row['student_attendance_data_unique_key']))
                conn.commit()
                inserted_count += 1
            except mysql.connector.Error as err:
                if err.errno == 1062:  # Duplicate entry error
                    duplicate_count += 1
                    logging.warning(f"Duplicate entry skipped: {row['student_attendance_data_unique_key']}")
                else:
                    error_count += 1
                    logging.error(f"Database error for row {row['student_attendance_data_unique_key']}: {err}")
                    raise

        # Update statistics
        stats['new_records_inserted'] = inserted_count
        stats['duplicate_records_skipped'] = duplicate_count
        stats['error_records'] = error_count

        logging.info(f"Database insertion completed:")
        logging.info(f"  - New records inserted: {inserted_count}")
        logging.info(f"  - Duplicate records skipped: {duplicate_count}")
        logging.info(f"  - Error records: {error_count}")

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
        stats['error_records'] = len(df)  # All records failed
    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

def fetch_data_from_api_and_process():
    from dateutil.relativedelta import relativedelta
    
    # Initialize statistics tracking
    stats = {
        'api_records_fetched': 0,
        'records_after_cleaning': 0,
        'existing_records_in_db': 0,
        'new_records_to_process': 0,
        'duplicates_removed': 0,
        'new_records_inserted': 0,
        'duplicate_records_skipped': 0,
        'error_records': 0,
        'processing_time_seconds': 0
    }
    
    start_time = datetime.now()
    
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
    logging.info(f"Requesting URL: {url_with_params}")

    response = requests.get(url_with_params, verify=False)

    if response.status_code == 200:
        json_response = response.json()
        data = json_response.get('data', [])
        stats['api_records_fetched'] = len(data) if isinstance(data, list) else 0
        logging.info(f"Data fetched from API successfully. Records fetched: {stats['api_records_fetched']}")

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
            
            stats['records_after_cleaning'] = len(df)
            logging.info(f"Records after data cleaning: {stats['records_after_cleaning']}")

            # Create unique keys for the API data BEFORE deduplication
            df['student_attendance_data_unique_key'] = df.apply(
                lambda row: f"{row['academic_year']}|{row['month']}|{row['school_name']}|{row['grade_name']}|{row['student_id']}",
                axis=1
            )

            # Get existing unique keys from database
            unique_keys_from_mysql = fetch_unique_keys_from_mysql()
            stats['existing_records_in_db'] = len(unique_keys_from_mysql)
            logging.info(f"Existing records in database: {stats['existing_records_in_db']}")

            # Filter out rows that already exist in the database FIRST
            new_data_df = df[~df['student_attendance_data_unique_key'].isin(unique_keys_from_mysql)]
            stats['new_records_to_process'] = len(new_data_df)
            logging.info(f"New records to process: {stats['new_records_to_process']}")

            # Then deduplicate the new data only
            if not new_data_df.empty:
                original_count = len(new_data_df)
                new_data_df = deduplicate_by_studentid(new_data_df)
                stats['duplicates_removed'] = original_count - len(new_data_df)
                logging.info(f"Duplicates removed during processing: {stats['duplicates_removed']}")

            if not new_data_df.empty:
                insert_data_to_mysql(new_data_df, stats)
                logging.info(f"Data processing completed for {month_name} {academic_year}.")
            else:
                logging.info(f"No new data to insert for {month_name} {academic_year}.")
        else:
            logging.info(f"No data available for {academic_year}, {month_name}")
    else:
        logging.error(f"Failed to retrieve data - Status code: {response.status_code}")
    
    # Calculate processing time
    end_time = datetime.now()
    stats['processing_time_seconds'] = (end_time - start_time).total_seconds()
    
    # Print comprehensive statistics
    print_statistics(stats, academic_year, month_name)
    
    return stats

def print_statistics(stats, academic_year, month_name):
    """Print comprehensive statistics about the data processing"""
    
    print("\n" + "="*80)
    print(f"📊 STUDENT ATTENDANCE ETL STATISTICS - {month_name} {academic_year}")
    print("="*80)
    
    print(f"\n📥 DATA SOURCE:")
    print(f"   • Records fetched from API: {stats['api_records_fetched']:,}")
    print(f"   • Records after cleaning: {stats['records_after_cleaning']:,}")
    
    print(f"\n🗄️  DATABASE STATUS:")
    print(f"   • Existing records in database: {stats['existing_records_in_db']:,}")
    print(f"   • New records to process: {stats['new_records_to_process']:,}")
    
    print(f"\n🔄 PROCESSING RESULTS:")
    print(f"   • Duplicates removed: {stats['duplicates_removed']:,}")
    print(f"   • New records inserted: {stats['new_records_inserted']:,}")
    print(f"   • Duplicate records skipped: {stats['duplicate_records_skipped']:,}")
    print(f"   • Error records: {stats['error_records']:,}")
    
    # Calculate success rate
    total_processed = stats['new_records_inserted'] + stats['duplicate_records_skipped'] + stats['error_records']
    if total_processed > 0:
        success_rate = (stats['new_records_inserted'] + stats['duplicate_records_skipped']) / total_processed * 100
        print(f"\n✅ SUCCESS RATE: {success_rate:.1f}%")
    
    print(f"\n⏱️  PERFORMANCE:")
    print(f"   • Processing time: {stats['processing_time_seconds']:.2f} seconds")
    if stats['new_records_inserted'] > 0:
        records_per_second = stats['new_records_inserted'] / stats['processing_time_seconds']
        print(f"   • Processing speed: {records_per_second:.1f} records/second")
    
    print("\n" + "="*80)
    
    # Log the same statistics
    logging.info("="*80)
    logging.info(f"STUDENT ATTENDANCE ETL STATISTICS - {month_name} {academic_year}")
    logging.info("="*80)
    logging.info(f"Records fetched from API: {stats['api_records_fetched']:,}")
    logging.info(f"Records after cleaning: {stats['records_after_cleaning']:,}")
    logging.info(f"Existing records in database: {stats['existing_records_in_db']:,}")
    logging.info(f"New records to process: {stats['new_records_to_process']:,}")
    logging.info(f"Duplicates removed: {stats['duplicates_removed']:,}")
    logging.info(f"New records inserted: {stats['new_records_inserted']:,}")
    logging.info(f"Duplicate records skipped: {stats['duplicate_records_skipped']:,}")
    logging.info(f"Error records: {stats['error_records']:,}")
    logging.info(f"Processing time: {stats['processing_time_seconds']:.2f} seconds")
    logging.info("="*80)

def main():
    logging.info("Starting daily update process.")
    
    # Setup database first
    setup_database()
    
    # Fetch and process data
    stats = fetch_data_from_api_and_process()
    
    logging.info("Daily update process completed successfully.")
    
    # Print final summary
    print(f"\n🎉 ETL PROCESS COMPLETED SUCCESSFULLY!")
    print(f"   Total new records added: {stats['new_records_inserted']:,}")
    print(f"   Processing time: {stats['processing_time_seconds']:.2f} seconds")

if __name__ == "__main__":
    main()
