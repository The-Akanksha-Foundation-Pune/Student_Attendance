import re
import logging
from logging_config import setup_logging
from datetime import datetime
import pandas as pd
setup_logging()

def clean_student_name(value):
    return re.sub(r'\s+', " ", value).strip().title()

def convert_grade_name(value):
    if not value or not isinstance(value, str):
        return value

    original_value = value
    cleaned = value.strip().upper().replace(".", "").replace(" ", "")

    # Known variants
    nursery_variants = {"NURSERY", "NURSARY", "NURSRY", "NURSEY", "NURSERRRY", "NUR"}
    jrkg_variants = {"JRKG", "JRKGCLASS", "JRKGKIDS", "JRKINDERGARTEN"}
    srkg_variants = {"SRKG", "SRKGCLASS", "SRKGKIDS", "SRKINDERGARTEN"}

    if cleaned in nursery_variants:
        return "NURSERY"
    elif cleaned in jrkg_variants:
        return "JR.KG"
    elif cleaned in srkg_variants:
        return "SR.KG"

    # Roman numeral → Number mapping up to XII
    roman_to_number = {
        "I": "1", "II": "2", "III": "3", "IV": "4", "V": "5",
        "VI": "6", "VII": "7", "VIII": "8", "IX": "9", "X": "10",
        "XI": "11", "XII": "12"
    }

    value_upper = value.strip().upper()

    # Match: "GRADE <roman>" or "GRADE <number>" with any spacing
    roman_match = re.match(r"GRADE\s+([IVXLCDM]+)$", value_upper, re.IGNORECASE)
    number_match = re.match(r"GRADE\s+(\d+)$", value_upper, re.IGNORECASE)

    if roman_match:
        roman = roman_match.group(1).upper()
        grade_number = roman_to_number.get(roman)
        if grade_number:
            return f"GRADE {grade_number}"
        else:
            return f"GRADE {roman}"  # Unrecognized Roman (like XIII)
    elif number_match:
        return f"GRADE {int(number_match.group(1))}"  # Normalize to GRADE <num>

    return original_value.strip().upper()

def format_date_column(original_date):
    try:
        formatted_date = datetime.strptime(original_date, '%d/%m/%Y').strftime('%Y-%m-%d')
        return formatted_date
    except ValueError:
        logging.error(f"Invalid date format: {original_date}")
        return original_date

def clean_gender(value):
    if not value or not isinstance(value, str):
        return value  # keep original value if it's not valid

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
    
import logging

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




