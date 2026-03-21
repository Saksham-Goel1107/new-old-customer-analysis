"""
Entrypoint for Total Customer Base Analysis service with scheduler, heartbeat and retry
"""
import os
import logging
import time
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from total_analysis import TotalCustomerAnalysis

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(), logging.FileHandler('total_analysis.log')]
)
logger = logging.getLogger(__name__)


def run_analysis():
    logger.info('🔄 Starting Total Customer Base Analysis run at %s', datetime.now())
    credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE', '/app/credentials.json')
    input_sheet_id = os.getenv('INPUT_SHEET_ID')
    output_sheet_id = os.getenv('OUTPUT_SHEET_ID')
    input_sheet_name = os.getenv('INPUT_SHEET_NAME', 'Sales Data')
    heartbeat_url = os.getenv('HEARTBEAT_URL', 'https://uptime.betterstack.com/api/v1/heartbeat/aZ3cu9UpPN3fXD3ZeTbf3ZvF')
    retry_count = int(os.getenv('RETRY_COUNT', '3'))
    retry_backoff = int(os.getenv('RETRY_BACKOFF_SECONDS', '30'))

    if not all([input_sheet_id, output_sheet_id]):
        logger.error('Missing INPUT_SHEET_ID or OUTPUT_SHEET_ID')
        return
    if not os.path.exists(credentials_file):
        logger.error('Credentials file not found: %s', credentials_file)
        return

    def send_heartbeat(success: bool, code: int | None = None):
        try:
            url = heartbeat_url
            if not success:
                url = url.rstrip('/') + ('/fail' if code is None else f'/{code}')
            resp = requests.head(url, timeout=10)
            logger.info('Heartbeat sent: %s -> %s', url, resp.status_code)
        except Exception as e:
            logger.warning('Failed to send heartbeat: %s', e)

    analysis = TotalCustomerAnalysis(credentials_file, input_sheet_id, input_sheet_name)
    attempt = 0
    success = False
    last_exc = None
    while attempt < retry_count and not success:
        attempt += 1
        try:
            logger.info('Attempt %d/%d', attempt, retry_count)
            success = analysis.run(output_sheet_id)
            if success:
                send_heartbeat(True)
                logger.info('✅ Total analysis completed successfully')
                return
            else:
                logger.error('Attempt %d failed (returned False)', attempt)
        except Exception as e:
            last_exc = e
            logger.exception('Attempt %d raised exception: %s', attempt, e)
        if attempt < retry_count:
            backoff = retry_backoff * (2 ** (attempt - 1))
            logger.info('Retrying in %s seconds...', backoff)
            time.sleep(backoff)

    if not success:
        exit_code = 2 if last_exc else 1
        send_heartbeat(False, exit_code)
        logger.error('❌ Total analysis failed after retries')
        if last_exc:
            logger.exception(last_exc)


def main():
    run_mode = os.getenv('RUN_MODE', 'scheduled').lower()
    if run_mode == 'once':
        run_analysis()
    elif run_mode == 'scheduled':
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            func=run_analysis,
            trigger=CronTrigger(hour=7, minute=0, timezone='UTC'),
            id='total_customer_daily',
            name='Total Customer Base Daily',
            replace_existing=True
        )
        scheduler.start()
        logger.info('⏰ Scheduler started - total analysis will run daily at 07:00 UTC')
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info('Shutting down scheduler...')
            scheduler.shutdown()
    else:
        logger.error('Unknown RUN_MODE: %s', run_mode)


if __name__ == '__main__':
    main()
