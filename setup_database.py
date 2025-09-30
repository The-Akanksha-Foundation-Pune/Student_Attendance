#!/usr/bin/env python3
"""
Database setup script for Student Attendance ETL
Creates the required MySQL table if it doesn't exist
"""

import mysql.connector
import logging
from configparser import ConfigParser
from logging_config import setup_logging

setup_logging()

def create_database_table():
    """Create the student_attendance_data table if it doesn't exist"""
    
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
    
    try:
        # Connect to MySQL
        logging.info("Connecting to MySQL database...")
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()
        
        # Read the SQL file
        with open('sql.txt', 'r') as file:
            create_table_sql = file.read()
        
        # Execute the table creation
        logging.info("Creating student_attendance_data table...")
        cursor.execute(create_table_sql)
        conn.commit()
        
        # Add unique key column
        logging.info("Adding unique key column...")
        cursor.execute("ALTER TABLE student_attendance_data ADD COLUMN student_attendance_data_unique_key VARCHAR(500) UNIQUE;")
        conn.commit()
        
        logging.info("Table 'student_attendance_data' created successfully!")
        
        # Verify table exists
        cursor.execute("SHOW TABLES LIKE 'student_attendance_data'")
        result = cursor.fetchone()
        if result:
            logging.info("Table verification successful!")
        else:
            logging.error("Table verification failed!")
            
    except mysql.connector.Error as err:
        if err.errno == 1050:  # Table already exists
            logging.info("Table 'student_attendance_data' already exists!")
        else:
            logging.error(f"Database error: {err}")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    create_database_table()
