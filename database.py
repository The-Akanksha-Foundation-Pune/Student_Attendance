import mysql.connector
import logging
from configparser import ConfigParser
from logging_config import setup_logging
from datetime import datetime

setup_logging()

# Initialize config parser
config = ConfigParser()
config.read('config.ini')

# Access MySQL configuration
db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'port': config['mysql']['port'],
    'database': config['mysql']['database']
}

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

def insert_data_to_mysql(df):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO student_attendance_data (
        school_name, grade_name, division_name, course_name, student_id, student_name, academic_year, gender, month, date, no_of_working_days, no_of_present_days, attendance_percentage,
        student_attendance_data_unique_key) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"""
        inserted_rows = []
        inserted_count = 0

        for _, row in df.iterrows():
            try:
                cursor.execute(insert_query, (
                    row['school_name'], row['grade_name'], row['division_name'], row['course_name'],
                    row['student_id'], row['student_name'], row['academic_year'], row['gender'], row['month'], row['date'], row['no_of_working_days'], row['no_of_present_days'],
                    row['attendance_percentage'], row['student_attendance_data_unique_key']))
                conn.commit()
                inserted_rows.append(row.to_dict())
                inserted_count += 1
            except mysql.connector.Error as err:
                if err.errno == 1062:  # Duplicate entry error
                    logging.warning(f"Duplicate entry skipped: {row['student_attendance_data_unique_key']}")
                else:
                    logging.error(f"Database error for row {row['student_attendance_data_unique_key']}: {err}")
                    raise

        logging.info(f"Total new records inserted: {inserted_count}")
        logging.info("Inserted rows:")
        for inserted_row in inserted_rows:
            logging.info(inserted_row)  # Log each inserted row

    except mysql.connector.Error as err:
        logging.error(f"Database error: {err}")
    finally:
        if conn.is_connected():
            cursor.close()  # Fixed indentation here
            conn.close()

def get_schools_with_data(academic_year, month_name):
    conn = mysql.connector.connect(**db_config)
    cursor = conn.cursor(dictionary=True)

    query = """
        SELECT DISTINCT school_name
        FROM student_attendance_data
        WHERE academic_year = %s AND month = %s
    """
    cursor.execute(query, (academic_year, month_name))
    result = [row['school_name'] for row in cursor.fetchall()]

    cursor.close()
    conn.close()
    return result
