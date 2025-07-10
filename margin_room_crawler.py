"""Fetch margin room for stock codes from FireAnt API and store into PostgreSQL."""

import logging
import os
from datetime import datetime
from typing import List, Tuple

import json
import requests
from bs4 import BeautifulSoup
import psycopg2

# Configure logging
LOG_FILE = './crawler.log'
logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

FA_API_BASE = os.getenv('FA_API_BASE', 'https://restv2.fireant.vn')
FA_TOKEN = os.getenv('FA_TOKEN', '')


def fetch_margin_data(stock_code: str) -> Tuple[float, str]:
    """Fetch margin room and sector for a stock from FireAnt API."""
    url = f"{FA_API_BASE}/stocks/{stock_code}?token={FA_TOKEN}"
    resp = requests.get(url, timeout=10)
    resp.raise_for_status()
    # Parse response text with BeautifulSoup before loading JSON
    soup = BeautifulSoup(resp.text, 'html.parser')
    data = json.loads(soup.get_text())
    margin_room = data.get('marginRoom') or data.get('margin_room')
    sector = data.get('industryName') or data.get('sector')
    if margin_room is None:
        raise RuntimeError(f'Margin room not found in response for {stock_code}')
    return margin_room, sector or ''


def save_margin_details(conn_params: dict, rows: List[Tuple[datetime, str, float, str]]):
    """Save margin room details into PostgreSQL."""
    conn = psycopg2.connect(**conn_params)
    try:
        with conn.cursor() as cur:
            cur.execute(
                """CREATE TABLE IF NOT EXISTS room_margin_detail (
                        date_time TIMESTAMP,
                        stock_code TEXT,
                        margin_room REAL,
                        sector TEXT,
                        PRIMARY KEY (date_time, stock_code)
                   )"""
            )
            for row in rows:
                cur.execute(
                    """INSERT INTO room_margin_detail(date_time, stock_code, margin_room, sector)
                       VALUES (%s, %s, %s, %s)
                       ON CONFLICT (date_time, stock_code) DO UPDATE
                         SET margin_room = EXCLUDED.margin_room,
                             sector = EXCLUDED.sector""",
                    row
                )
        conn.commit()
        logger.info('Saved %d margin records', len(rows))
    finally:
        conn.close()


def main():
    stock_list = os.getenv('STOCK_LIST', '')
    if stock_list:
        stock_codes = [code.strip() for code in stock_list.split(',') if code.strip()]
    else:
        logger.error('No STOCK_LIST provided')
        return
    conn_params = {
        'host': os.getenv('DB_HOST', 'localhost'),
        'database': os.getenv('DB_NAME', 'postgres'),
        'user': os.getenv('DB_USER', 'postgres'),
        'password': os.getenv('DB_PASS', ''),
        'port': int(os.getenv('DB_PORT', 5432)),
    }
    rows = []
    for code in stock_codes:
        try:
            margin_room, sector = fetch_margin_data(code)
            rows.append((datetime.now(), code, margin_room, sector))
            logger.info('Fetched margin for %s: %s', code, margin_room)
        except Exception as exc:
            logger.exception('Failed to fetch margin for %s: %s', code, exc)
    if rows:
        try:
            save_margin_details(conn_params, rows)
        except Exception as exc:
            logger.exception('Failed to save margin details: %s', exc)


def cron_job():
    """Entry point for cron."""
    logger.info('Start crawling margin room')
    main()
    logger.info('Finished crawling margin room')


if __name__ == '__main__':
    cron_job()
