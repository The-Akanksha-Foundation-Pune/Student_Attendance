import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
from configparser import ConfigParser

# Load email config
config = ConfigParser()
config.read('config.ini')

SMTP_SERVER = config['email']['SMTP_SERVER']
SMTP_PORT = int(config['email']['SMTP_PORT'])
SMTP_USERNAME = config['email']['SMTP_USERNAME']
SMTP_PASSWORD = config['email']['SMTP_PASSWORD']

def send_email(to, subject, body):
    try:
        msg = MIMEMultipart()
        msg['From'] = SMTP_USERNAME
        msg['To'] = to
        msg['Subject'] = subject

        msg.attach(MIMEText(body, 'plain'))

        with smtplib.SMTP(SMTP_SERVER, SMTP_PORT) as server:
            server.starttls()
            server.login(SMTP_USERNAME, SMTP_PASSWORD)
            server.send_message(msg)

        logging.info(f"Email sent to {to} with subject: {subject}")

    except Exception as e:
        logging.error(f"Failed to send email to {to}: {e}")
