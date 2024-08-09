import psycopg2
import mysql.connector
from config import *
import threading
import time
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime
import signal
import sys

# Flag untuk menghentikan thread
stop_threads = False

def fetch_data_from_postgres():
    try:
        print("Connecting to PostgreSQL...")
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            database=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()
        print("Connected to PostgreSQL. Fetching data from sla_coba table...")
        cur.execute("SELECT * FROM sla_coba")
        rows = cur.fetchall()
        print(f"Fetched {len(rows)} rows from sla_coba table.")
        cur.close()
        conn.close()
        print("Closed connection to PostgreSQL.")
        return rows
    except Exception as e:
        print(f"Error fetching data from PostgreSQL: {e}")
        return []

def insert_data_into_mysql(data):
    try:
        print("Connecting to MySQL...")
        # Connect to MySQL
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        cur = conn.cursor()
        print("Connected to MySQL. Inserting data into sla_coba table...")
        insert_query = """
        INSERT INTO sla_coba (id, bank, asset_tag, numbers, sub_category_name, item_name, item_category_name, state, service_hour, opened, sendback, validate, resolved, closed, imported_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        cur.executemany(insert_query, data)
        conn.commit()
        inserted_count = cur.rowcount
        print(f"Inserted {inserted_count} rows into sla_coba table.")
        cur.close()
        conn.close()
        print("Closed connection to MySQL.")
        return inserted_count
    except Exception as e:
        print(f"Error inserting data into MySQL: {e}")
        return 0

def truncate_mysql_table():
    try:
        print("Connecting to MySQL for truncation...")
        # Connect to MySQL
        conn = mysql.connector.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DB
        )
        cur = conn.cursor()
        print("Connected to MySQL. Truncating sla_coba table...")
        cur.execute("TRUNCATE TABLE sla_coba")
        conn.commit()
        print("Truncated sla_coba table.")
        cur.close()
        conn.close()
        print("Closed connection to MySQL.")
    except Exception as e:
        print(f"Error truncating table in MySQL: {e}")

def send_email(entry_count, entry_time):
    from_email = "todolistbyhamja@gmail.com"
    to_email = "kinelly42@gmail.com"
    password = ""

    msg = MIMEMultipart()
    msg['From'] = from_email
    msg['To'] = to_email
    msg['Subject'] = "Data Migration Report"

    body = f"""
    <h3>Data Migration Report</h3>
    <table border="1">
        <tr>
            <th>Jumlah Data Masuk</th>
            <th>Waktu Masuk</th>
        </tr>
        <tr>
            <td>{entry_count}</td>
            <td>{entry_time}</td>
        </tr>
    </table>
    """

    msg.attach(MIMEText(body, 'html'))

    try:
        server = smtplib.SMTP('smtp.gmail.com', 587)
        server.starttls()
        server.login(from_email, password)
        text = msg.as_string()
        server.sendmail(from_email, to_email, text)
        server.quit()
        print("Email sent successfully!")
    except Exception as e:
        print(f"Failed to send email: {e}")

def migrate_data_periodically():
    global stop_threads
    while not stop_threads:
        print("Starting data migration...")
        data = fetch_data_from_postgres()
        if data:
            inserted_count = insert_data_into_mysql(data)
            entry_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            send_email(inserted_count, entry_time)
            print("Data migration completed successfully!")
        else:
            print("No data to migrate.")
        time.sleep(120)  # Wait for 2 minutes

def truncate_table_periodically():
    global stop_threads
    while not stop_threads:
        time.sleep(300)  # Wait for 5 minutes
        truncate_mysql_table()

def signal_handler(sig, frame):
    global stop_threads
    print('Stopping threads...')
    stop_threads = True

if __name__ == "__main__":
    signal.signal(signal.SIGINT, signal_handler)
    print("Starting periodic tasks... Press Ctrl+C to stop.")
    migration_thread = threading.Thread(target=migrate_data_periodically)
    truncation_thread = threading.Thread(target=truncate_table_periodically)

    migration_thread.start()
    truncation_thread.start()

    migration_thread.join()
    truncation_thread.join()
    print("Program terminated.")
