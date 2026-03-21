"""
Application entry point with APScheduler for daily execution at 7am UTC
"""

import os
import logging
import time
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
from cohort_analysis import CohortAnalysis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('cohort_analysis.log')
    ]
)
logger = logging.getLogger(__name__)


def run_analysis():
    """Execute the retention cohort analysis"""
    try:
        logger.info(f"🔄 Starting scheduled analysis at {datetime.now()}")
        
        credentials_file = os.getenv('GOOGLE_CREDENTIALS_FILE', '/app/credentials.json')
        input_sheet_id = os.getenv('INPUT_SHEET_ID')
        output_sheet_id = os.getenv('OUTPUT_SHEET_ID')
        input_sheet_name = os.getenv('INPUT_SHEET_NAME', 'Sales Data')
        # Heartbeat + retry configuration
        heartbeat_url = os.getenv('HEARTBEAT_URL', 'https://uptime.betterstack.com/api/v1/heartbeat/aZ3cu9UpPN3fXD3ZeTbf3ZvF')
        retry_count = int(os.getenv('RETRY_COUNT', '3'))
        retry_backoff = int(os.getenv('RETRY_BACKOFF_SECONDS', '30'))
        
        if not all([input_sheet_id, output_sheet_id]):
            logger.error(
                "Missing required environment variables: "
                "INPUT_SHEET_ID, OUTPUT_SHEET_ID"
            )
            return
        
        if not os.path.exists(credentials_file):
            logger.error(f"Credentials file not found: {credentials_file}")
            return

        def send_heartbeat(success: bool, code: int | None = None):
            """Send a simple heartbeat to the monitoring URL.

            On failure, append /fail or /<exit-code> to the URL per BetterStack.
            """
            try:
                url = heartbeat_url
                if not success:
                    if code is None:
                        url = url.rstrip('/') + '/fail'
                    else:
                        url = url.rstrip('/') + f'/{code}'
                # Use HEAD for lightweight check
                resp = requests.head(url, timeout=10)
                logger.info(f"Heartbeat sent: {url} -> {resp.status_code}")
            except Exception as e:
                logger.warning(f"Failed to send heartbeat: {e}")

        analysis = CohortAnalysis(credentials_file, input_sheet_id, input_sheet_name)

        attempt = 0
        success = False
        last_exception = None
        while attempt < retry_count and not success:
            attempt += 1
            try:
                logger.info(f"Attempt {attempt}/{retry_count} to run analysis")
                success = analysis.run(output_sheet_id)
                if success:
                    send_heartbeat(True)
                    logger.info("✅ Analysis completed successfully")
                    return
                else:
                    logger.error(f"Attempt {attempt} failed (analysis.run returned False)")
            except Exception as e:
                last_exception = e
                logger.exception(f"Attempt {attempt} raised exception: {e}")

            if attempt < retry_count:
                backoff = retry_backoff * (2 ** (attempt - 1))
                logger.info(f"Retrying in {backoff} seconds...")
                time.sleep(backoff)

        # After retries exhausted
        if not success:
            # Report failure with exit code if exception occurred
            exit_code = 2 if last_exception else 1
            send_heartbeat(False, exit_code)
            logger.error("❌ Analysis failed after retries")
            if last_exception:
                logger.exception(last_exception)
            return
    except Exception as e:
        logger.error(f"Error during scheduled execution: {e}", exc_info=True)


def main():
    """Initialize scheduler or run once depending on mode"""
    run_mode = os.getenv('RUN_MODE', 'scheduled').lower()
    
    if run_mode == 'once':
        logger.info("Running in ONCE mode - executing immediately")
        run_analysis()
    elif run_mode == 'scheduled':
        logger.info("Running in SCHEDULED mode - starting scheduler")
        
        scheduler = BackgroundScheduler()
        
        # Daily at 7:00 AM UTC
        scheduler.add_job(
            func=run_analysis,
            trigger=CronTrigger(hour=7, minute=0, timezone='UTC'),
            id='cohort_analysis_daily',
            name='Daily Cohort Retention Analysis',
            replace_existing=True
        )
        
        scheduler.start()
        logger.info("⏰ Scheduler started - analysis will run daily at 07:00 UTC")
        logger.info("Press Ctrl+C to exit")
        
        try:
            # Keep the scheduler running
            while True:
                import time
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down scheduler...")
            scheduler.shutdown()
            logger.info("Scheduler stopped")
    else:
        logger.error(f"Unknown RUN_MODE: {run_mode}. Use 'once' or 'scheduled'")


if __name__ == '__main__':
    main()
