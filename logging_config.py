import logging
import logging.config
from datetime import datetime
import os

def setup_logging():
    # Generate log filename
    current_date = datetime.now()
    month_name = current_date.strftime('%B')
    week_number = current_date.strftime('%U')
    academic_year = f"{current_date.year}-{current_date.year + 1}" if current_date.month >= 5 else f"{current_date.year - 1}-{current_date.year}"
    log_filename = f"{month_name}_week{week_number}_{academic_year}.log"

    # Log file directory - use current directory for macOS compatibility
    log_dir = os.path.join(os.getcwd(), "logs")
    os.makedirs(log_dir, exist_ok=True)

    # Full path to the log file
    log_file_path = os.path.join(log_dir, log_filename)
    logging.config.dictConfig({
        'version': 1,
        'disable_existing_loggers': False,
        'formatters': {
            'default': {
                'format': '%(asctime)s - %(levelname)s - %(message)s',
            },
        },
        'handlers': {
            'file': {
                'level': 'INFO',
                'class': 'logging.FileHandler',
                'filename': log_file_path,
                'formatter': 'default',
            },
            'console': {
                'level': 'INFO',
                'class': 'logging.StreamHandler',
                'formatter': 'default',
            },
        },
        'root': {
            'level': 'INFO',
            'handlers': ['file', 'console'],
        },
    })

# Initialize logging when this module is imported
setup_logging()