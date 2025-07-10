"""Fetch USD/VND and CNY/VND exchange rates from Vietcombank.
Store results into PostgreSQL.
"""

import logging
import os
from datetime import datetime
from io import StringIO

import requests
import pandas as pd
import psycopg2

# Configure logging
LOG_FILE = './crawler.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_fx_rates():
    """Fetch USD/VND and CNY/VND rates from VCB site."""
    url = "https://portal.vietcombank.com.vn/UserControls/TVPortal.TyGia/pXML.aspx"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch rates from VCB: {e}")
        raise

    try:
        df = pd.read_xml(StringIO(resp.text))
    except Exception as e:
        logger.error(f"Failed to parse XML from VCB: {e}")
        raise

    usd_row = df[df["CurrencyCode"] == "USD"]
    cny_row = df[df["CurrencyCode"] == "CNY"]

    if usd_row.empty or cny_row.empty:
        raise RuntimeError("USD or CNY rates not found in VCB data")

    usd_vnd = float(usd_row["Sell"].values[0].replace(",", ""))
    cny_vnd = float(cny_row["Sell"].values[0].replace(",", ""))

    logger.info(f"Fetched USD/VND from VCB: {usd_vnd}")
    logger.info(f"Fetched CNY/VND from VCB: {cny_vnd}")

    return datetime.now(), usd_vnd, cny_vnd

def save_fx_rate(conn_params, date_time, usd_vnd, cny_vnd):
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS fx_rate (
                    date_time TIMESTAMP PRIMARY KEY,
                    usd_vnd REAL,
                    cny_vnd REAL
                )"""
            )
            cur.execute(
                """INSERT INTO fx_rate(date_time, usd_vnd, cny_vnd)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (date_time) DO UPDATE
                     SET usd_vnd = EXCLUDED.usd_vnd,
                         cny_vnd = EXCLUDED.cny_vnd""",
                (date_time, usd_vnd, cny_vnd)
            )
        conn.commit()
        logger.info('Saved FX rates successfully.')
    finally:
        conn.close()

def main():
    try:
        date_time, usd_vnd, cny_vnd = fetch_fx_rates()
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
        save_fx_rate(conn_params, date_time, usd_vnd, cny_vnd)
    except Exception as exc:
        logger.exception('Failed to save FX rates: %s', exc)

def cron_job():
    """Entry point for cron."""
    logger.info('Start crawling FX rates')
    main()
    logger.info('Finished crawling FX rates')

if __name__ == '__main__':
    cron_job()
