import logging
from logging_config import setup_logging
from api import fetch_data_from_api_and_process, send_reminders_if_no_data
from datetime import datetime

setup_logging()

def main():
    logging.info("Starting daily update process.")
    fetch_data_from_api_and_process()
    logging.info("Daily update process completed successfully.")
    send_reminders_if_no_data()

if __name__ == "__main__":
    main()
