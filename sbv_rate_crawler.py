"""Crawl SBV O/N and 1W interbank rates.
Run this script via cron to store data into PostgreSQL.
"""
import requests
from bs4 import BeautifulSoup
import psycopg2
import logging
import os
from datetime import datetime

# Configure logging
LOG_FILE = '/var/log/crawler.log'
logging.basicConfig(filename=LOG_FILE,
                    level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def parse_rate(value: str) -> float:
    """Convert rate string with comma decimal separator to float."""
    try:
        return float(value.replace(',', '.'))
    except ValueError:
        logger.error('Cannot parse rate value: %s', value)
        raise

def fetch_rates(url: str):
    """Fetch O/N and 1W rates from SBV website."""
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    if not table:
        raise RuntimeError('No table found on page')
    on_rate = None
    onew_rate = None
    date = datetime.now().date()

    rows = table.find_all('tr')
    for row in rows:
        cells = [c.get_text(strip=True) for c in row.find_all(['th', 'td'])]
        if not cells:
            continue
        if 'O/N' in cells[0]:
            on_rate = parse_rate(cells[1])
        elif '1W' in cells[0] or '1 Tuần' in cells[0]:
            onew_rate = parse_rate(cells[1])
        if on_rate is not None and onew_rate is not None:
            break

    if on_rate is None or onew_rate is None:
        raise RuntimeError('Could not parse rates from page')
    return date, on_rate, onew_rate

def save_rate(conn_params, date, on_rate, onew_rate):
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS on_rate (
                    date_time DATE PRIMARY KEY,
                    on_rate REAL,
                    onew_rate REAL
                )"""
            )
            cur.execute(
                """INSERT INTO on_rate(date_time, on_rate, onew_rate)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (date_time) DO UPDATE
                     SET on_rate=EXCLUDED.on_rate,
                         onew_rate=EXCLUDED.onew_rate""",
                (date, on_rate, onew_rate)
            )
        conn.commit()
    finally:
        conn.close()

def main():
    url = 'https://sbv.gov.vn/lai-suat-lien-ngan-hang'
    try:
        date, on_rate, onew_rate = fetch_rates(url)
    except Exception as exc:
        logger.exception('Failed to fetch rates: %s', exc)
        return

    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS', ''),
        'port': int(os.getenv('DB_PORT', 5432)),
    }

    try:
        save_rate(conn_params, date, on_rate, onew_rate)
    except Exception as exc:
        logger.exception('Failed to save rates: %s', exc)

def cron_job():
    """Entry point for cron."""
    logger.info('Start crawling SBV rates')
    main()
    logger.info('Finished crawling SBV rates')

if __name__ == '__main__':
    cron_job()

# Example cron entry to run daily at 7 AM:
# 0 7 * * * /usr/bin/python3 /path/to/sbv_rate_crawler.py


