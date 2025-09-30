import requests
import pandas as pd
from datetime import datetime
import logging
import re
import mysql.connector
import configparser
import warnings
import urllib3

warnings.filterwarnings("ignore", category=UserWarning, module="pandas")

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure logging
logging.basicConfig(
    filename='student_attendance_yearly_data.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# Read config.ini
config = configparser.ConfigParser()
config.read('config.ini')

# API details
api_url = config['api']['url']
api_key = config['api']['key']

# MySQL config
db_config = {
    'user': config['mysql']['user'],
    'password': config['mysql']['password'],
    'host': config['mysql']['host'],
    'port': config['mysql']['port'],
    'database': config['mysql']['database']
}

# ------------------------ Cleaning Functions ------------------------

def clean_student_name(value):
    return re.sub(r'\s+', " ", value).strip().title()

def convert_grade_name(value):
    if not value or not isinstance(value, str):
        return value
    original_value = value
    cleaned = value.strip().upper().replace(".", "").replace(" ", "")

    nursery_variants = {"NURSERY", "NURSARY", "NURSRY", "NURSEY", "NURSERRRY", "NUR"}
    jrkg_variants = {"JRKG", "JRKGCLASS", "JRKGKIDS", "JRKINDERGARTEN"}
    srkg_variants = {"SRKG", "SRKGCLASS", "SRKGKIDS", "SRKINDERGARTEN"}

    if cleaned in nursery_variants:
        return "NURSERY"
    elif cleaned in jrkg_variants:
        return "JR.KG"
    elif cleaned in srkg_variants:
        return "SR.KG"

    roman_to_number = {
        "I": "1", "II": "2", "III": "3", "IV": "4", "V": "5",
        "VI": "6", "VII": "7", "VIII": "8", "IX": "9", "X": "10",
        "XI": "11", "XII": "12"
    }
    value_upper = value.strip().upper()
    roman_match = re.match(r"GRADE\s+([IVXLCDM]+)$", value_upper)
    number_match = re.match(r"GRADE\s+(\d+)$", value_upper)

    if roman_match:
        roman = roman_match.group(1).upper()
        grade_number = roman_to_number.get(roman)
        return f"GRADE {grade_number}" if grade_number else f"GRADE {roman}"
    elif number_match:
        return f"GRADE {int(number_match.group(1))}"

    return original_value.strip().upper()

def format_date_column(original_date):
    try:
        return datetime.strptime(original_date, '%d/%m/%Y').strftime('%Y-%m-%d')
    except ValueError:
        logging.error(f"Invalid date format: {original_date}")
        return original_date

def extract_division(value):
    match = re.search(r'[A-Za-z]+', value)
    return match.group(0) if match else value

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

def fill_na(df):
    return df.fillna("NA")

def deduplicate_by_studentid(df):
    duplicated_subset = df.duplicated(subset=['student_id', 'student_name'], keep=False)
    df_duplicates = df[duplicated_subset].copy()
    df_duplicates.sort_values(
        by=['student_id', 'student_name', 'attendance_percentage'],
        ascending=[True, True, False],
        inplace=True
    )
    df_deduped = df_duplicates.drop_duplicates(subset=['student_id', 'student_name'], keep='first')
    df_non_duplicates = df[~duplicated_subset]
    return pd.concat([df_non_duplicates, df_deduped], ignore_index=True)

def trim_dataframe(df):
    for col in df.select_dtypes(include=['object']):
        df[col] = df[col].map(lambda x: x.strip() if isinstance(x, str) else x)
    return df


# ------------------------ Database Insert ------------------------

def insert_data_to_mysql(df):
    try:
        conn = mysql.connector.connect(**db_config)
        cursor = conn.cursor()

        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS student_attendance_data (
            id INT(11) NOT NULL AUTO_INCREMENT PRIMARY KEY,
            school_name VARCHAR(50),
            grade_name VARCHAR(50),
            division_name VARCHAR(10),
            course_name VARCHAR(10),
            student_id VARCHAR(50),
            student_name VARCHAR(255),
            academic_year VARCHAR(15),
            gender VARCHAR(10),
            month VARCHAR(15),
            date DATE,
            no_of_working_days INT(11),
            no_of_present_days INT(11),
            attendance_percentage FLOAT,
            student_attendance_data_unique_key VARCHAR(150) UNIQUE,
            timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
            
            -- Indexes for faster lookups
            INDEX idx_school_name (school_name),
            INDEX idx_grade_name (grade_name),
            INDEX idx_student_id (student_id),
            INDEX idx_academic_year (academic_year),
            INDEX idx_month (month),
            INDEX idx_date (date),
            INDEX idx_school_grade_year (school_name, grade_name, academic_year),
            INDEX idx_student_year (student_id, academic_year)
        ) ENGINE=InnoDB DEFAULT CHARSET=latin1;

        """
        cursor.execute(create_table_sql)

        # Insert or update if duplicate key exists
        sql_insert = """
        INSERT INTO student_attendance_data (
            school_name, grade_name, division_name, course_name, student_id, student_name, academic_year,
            gender, month, date, no_of_working_days, no_of_present_days, attendance_percentage, student_attendance_data_unique_key
        ) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            school_name = VALUES(school_name),
            grade_name = VALUES(grade_name),
            division_name = VALUES(division_name),
            course_name = VALUES(course_name),
            student_name = VALUES(student_name),
            academic_year = VALUES(academic_year),
            gender = VALUES(gender),
            month = VALUES(month),
            date = VALUES(date),
            no_of_working_days = VALUES(no_of_working_days),
            no_of_present_days = VALUES(no_of_present_days),
            attendance_percentage = VALUES(attendance_percentage),
            timestamp = CURRENT_TIMESTAMP
        """

        for _, row in df.iterrows():
            cursor.execute(sql_insert, (
                row['school_name'], row['grade_name'], row['division_name'], row['course_name'], 
                row['student_id'], row['student_name'], row['academic_year'], row['gender'], 
                row['month'], row['date'], row['no_of_working_days'], row['no_of_present_days'], 
                row['attendance_percentage'], row['student_attendance_data_unique_key']
            ))

        conn.commit()
        print(f"Inserted/Updated {len(df)} records into the database.")
        logging.info(f"Inserted/Updated {len(df)} records into the database.")

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        logging.error(f"Database error: {err}")

    finally:
        if conn.is_connected():
            cursor.close()
            conn.close()

# ------------------------ Fetch Data ------------------------

def fetch_data(api_url, params, academic_year, month):
    url_with_params = f"{api_url}?api-key={params['api-key']}&academic_year={academic_year}&month_name={month}"
    print(f"Requesting URL: {url_with_params}")
    logging.info(f"Requesting URL: {url_with_params}")

    response = requests.get(url_with_params, verify=False)
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
            df['attendance_percentage'] = df['attendance_percentage'].round()

            df = fill_na(df)
            df = deduplicate_by_studentid(df)
            df = trim_dataframe(df)

            df['student_attendance_data_unique_key'] = df.apply(
                lambda row: f"{row['academic_year']}|{row['month']}|{row['school_name']}|{row['grade_name']}|{row['student_id']}",
                axis=1
            )

            insert_data_to_mysql(df)
            print(f"Data extracted for {academic_year}, {month}")
            logging.info(f"Data extracted for {academic_year}, {month}")
        else:
            print(f"No data available for {academic_year}, {month}")
            logging.warning(f"No data available for {academic_year}, {month}")
    else:
        print(f"Failed to retrieve data for {academic_year}, {month} - Status code: {response.status_code}")
        logging.error(f"Failed to retrieve data for {academic_year}, {month} - Status code: {response.status_code}")

# ------------------------ Main ------------------------

def main():
    current_year = datetime.now().year
    academic_years = [f'{year}-{year+1}' for year in range(2022, current_year+1)]
    all_months = ['May', 'June', 'July', 'August', 'September', 'October', 
                  'November', 'December', 'January', 'February', 'March', 'April']

    for academic_year in academic_years:
        for month in all_months:
            params = {
                'api-key': api_key,
                'academic_year': academic_year,
                'month_name': month
            }
            fetch_data(api_url, params, academic_year, month)

    print("Data extraction process completed.")
    logging.info("Data extraction process completed.")

if __name__ == "__main__":
    main()
