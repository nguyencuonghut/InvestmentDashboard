"""Fetch USD/VND and USD/CNY exchange rates from SBV website.
Store results into PostgreSQL.
"""
import logging
import os
from datetime import datetime

import requests
from bs4 import BeautifulSoup
import psycopg2

# Configure logging
LOG_FILE = '/var/log/crawler.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_rate(value: str) -> float:
    """Convert string to float handling comma decimal separator."""
    try:
        return float(value.replace(',', '.'))
    except ValueError:
        logger.error('Cannot parse rate: %s', value)
        raise

def fetch_fx_rates(url: str):
    """Return current datetime, USD/VND rate and USD/CNY rate from page."""
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, 'html.parser')
    table = soup.find('table')
    if not table:
        raise RuntimeError('No table found on page')
    usd_vnd = None
    usd_cny = None
    for row in table.find_all('tr'):
        cells = [c.get_text(strip=True) for c in row.find_all('td')]
        if not cells:
            continue
        text = ' '.join(cells).upper()
        if ('USD/VND' in text or ('USD' in text and 'VND' in text)) and len(cells) > 1:
            usd_vnd = parse_rate(cells[1])
        elif ('USD/CNY' in text or ('USD' in text and 'CNY' in text)) and len(cells) > 1:
            usd_cny = parse_rate(cells[1])
        if usd_vnd is not None and usd_cny is not None:
            break
    if usd_vnd is None or usd_cny is None:
        raise RuntimeError('Could not parse fx rates from page')
    return datetime.now(), usd_vnd, usd_cny

def save_fx_rate(conn_params, date_time, usd_vnd, usd_cny):
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS fx_rate (
                    date_time TIMESTAMP PRIMARY KEY,
                    usd_vnd REAL,
                    usd_cny REAL
                )"""
            )
            cur.execute(
                """INSERT INTO fx_rate(date_time, usd_vnd, usd_cny)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (date_time) DO UPDATE
                     SET usd_vnd = EXCLUDED.usd_vnd,
                         usd_cny = EXCLUDED.usd_cny""",
                (date_time, usd_vnd, usd_cny)
            )
        conn.commit()
    finally:
        conn.close()

def main():
    url = 'https://dttktt.sbv.gov.vn/TyGia/faces/TyGia.jspx?_afrLoop=27580426003077755&_afrWindowMode=0&_adf.ctrl-state=1bw6u8ql4s_4'
    try:
        date_time, usd_vnd, usd_cny = fetch_fx_rates(url)
    except Exception as exc:
        logger.exception('Failed to fetch FX rates: %s', exc)
        return
    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS', ''),
        'port': int(os.getenv('DB_PORT', 5432)),
    }
    try:
        save_fx_rate(conn_params, date_time, usd_vnd, usd_cny)
    except Exception as exc:
        logger.exception('Failed to save FX rates: %s', exc)

def cron_job():
    """Entry point for cron."""
    logger.info('Start crawling FX rates')
    main()
    logger.info('Finished crawling FX rates')

if __name__ == '__main__':
    cron_job()

# Example cron entry to run every day at 8 AM:
# 0 8 * * * /usr/bin/python3 /path/to/fx_rate_crawler.py
