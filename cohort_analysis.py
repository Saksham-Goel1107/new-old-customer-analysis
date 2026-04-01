"""
Monthly Customer Retention Cohort Analysis
Reads from Google Sheets, performs analysis, outputs to Google Sheets
"""

import pandas as pd
import numpy as np
from datetime import datetime
from google.oauth2.service_account import Credentials
from google.auth.transport.requests import Request
import gspread
import logging
from io import StringIO
from sheets_utils import open_spreadsheet, ensure_worksheet, clear_worksheet, append_rows

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CohortAnalysis:
    def __init__(self, credentials_file, input_sheet_id, input_sheet_name="main"):
        """
        Initialize the cohort analysis with Google Sheets credentials
        
        Args:
            credentials_file: Path to service account JSON file
            input_sheet_id: Google Sheet ID
            input_sheet_name: Name of the sheet with sales data
        """
        self.credentials_file = credentials_file
        self.input_sheet_id = input_sheet_id
        self.input_sheet_name = input_sheet_name
        self.gc = self._authenticate()
        logger.info("Google Sheets authentication successful")
    
    def _authenticate(self):
        """Authenticate with Google Sheets API"""
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        credentials = Credentials.from_service_account_file(
            self.credentials_file, scopes=scope
        )
        return gspread.authorize(credentials)
    
    def load_data_from_sheets(self):
        """Load sales data from Google Sheets"""
        try:
            logger.info(f"Opening sheet: {self.input_sheet_id}")
            worksheet = open_spreadsheet(self.gc, self.input_sheet_id)
            try:
                sheet = worksheet.worksheet(self.input_sheet_name)
            except Exception as e:
                from gspread.exceptions import WorksheetNotFound
                if isinstance(e, WorksheetNotFound):
                    titles = [w.title for w in worksheet.worksheets()]
                    logger.warning("Worksheet '%s' not found. Available sheets: %s. Falling back to first sheet.", self.input_sheet_name, titles)
                    if not titles:
                        raise
                    sheet = worksheet.worksheet(titles[0])
                else:
                    raise

            # Get all values
            data = sheet.get_all_records()
            logger.info(f"Loaded {len(data)} rows from Google Sheets")
            
            # Convert to DataFrame
            df = pd.DataFrame(data)
            
            # Clean and convert date column
            if 'date' in df.columns:
                df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
                df['date'] = df['date'].dt.tz_localize(None)
            
            return df
        except Exception as e:
            logger.error(f"Error loading data from Google Sheets: {e}")
            raise
    
    def clean_and_prepare(self, df):
        """Clean data and prepare for analysis"""
        logger.info("Cleaning and preparing data...")
        
        # Remove rows with invalid data
        df = df[
            (df['date'].notna()) & 
            (df['number'].notna()) & 
            (df['number'].astype(str).str.strip() != 'Total')
        ].copy()
        
        # Bill-level deduplication
        orders = df.drop_duplicates(subset='number').copy()
        orders = orders[[
            'number', 'date', 'customerMobile', 'customerName', 'orderAmount'
        ]].copy()
        
        # Clean customer identifiers
        orders['customerMobile'] = (
            orders['customerMobile']
            .astype(str)
            .str.strip()
            .str.lower()
        )
        orders['customerName'] = (
            orders['customerName']
            .astype(str)
            .str.strip()
            .str.lower()
        )
        
        logger.info(f"Total orders (unique bills): {len(orders):,}")
        return orders
    
    def resolve_customer_id(self, row):
        """Resolve customer ID from mobile or name"""
        INVALID = {'nan', '', 'none', '0', '0.0'}
        
        mob = row['customerMobile']
        name = row['customerName']
        
        if mob not in INVALID:
            return f"MOB:{mob}"
        elif name not in INVALID:
            return f"NAME:{name}"
        return None
    
    def create_customer_identifier(self, orders):
        """Create unified customer identifiers"""
        logger.info("Creating customer identifiers...")
        orders['customer_id'] = orders.apply(self.resolve_customer_id, axis=1)
        
        identified = orders[orders['customer_id'].notna()].copy()
        anon_count = orders['customer_id'].isna().sum()
        
        logger.info(f"Identified orders: {len(identified):,}")
        logger.info(f"Anonymous (excluded): {anon_count:,}")
        logger.info(f"Unique customers: {identified['customer_id'].nunique():,}")
        
        return identified
    
    def build_monthly_aggregates(self, identified):
        """Build monthly aggregates and cohorts"""
        logger.info("Building monthly aggregates...")
        
        # Create month periods
        identified['month'] = identified['date'].dt.to_period('M')
        
        data_months = sorted(identified['month'].dropna().unique())
        today_period = pd.Period(datetime.today(), freq='M')
        start = data_months[0]
        end = max(data_months[-1], today_period)
        all_months = list(pd.period_range(start=start, end=end, freq='M'))
        
        logger.info(f"Month range: {all_months[0]} → {all_months[-1]} ({len(all_months)} months)")
        
        # Cohort assignment
        first_purchase = (
            identified
            .groupby('customer_id')['month']
            .min()
            .rename('cohort_month')
        )
        identified = identified.join(first_purchase, on='customer_id')
        
        # Cohort sizes
        cohort_sizes = (
            identified
            .groupby('cohort_month')['customer_id']
            .nunique()
            .rename('new_customers')
        )
        
        # Total unique customers per month
        total_per_month = (
            identified
            .groupby('month')['customer_id']
            .nunique()
            .rename('total_customers')
        )
        
        # Returning customers per cohort × purchase month
        cohort_data = (
            identified
            .groupby(['cohort_month', 'month'])['customer_id']
            .nunique()
            .reset_index()
            .rename(columns={
                'month': 'purchase_month',
                'customer_id': 'returning_customers'
            })
        )
        
        return all_months, cohort_sizes, total_per_month, cohort_data, identified
    
    def get_retention_count(self, cohort, purchase_month, cohort_data):
        """Get retention count for a cohort in a purchase month"""
        mask = (
            (cohort_data['cohort_month'] == cohort) &
            (cohort_data['purchase_month'] == purchase_month)
        )
        return int(cohort_data.loc[mask, 'returning_customers'].sum())
    
    def build_matrices(self, all_months, cohort_sizes, total_per_month, cohort_data):
        """Build retention count and percentage matrices"""
        logger.info("Building retention matrices...")
        
        matrix_rows = []
        for cohort in all_months:
            n_new = int(cohort_sizes.get(cohort, 0))
            row = {
                'Activation Month': str(cohort),
                'New Customers': n_new
            }
            
            for m in all_months:
                if m < cohort:
                    row[str(m)] = ''
                elif m == cohort:
                    row[str(m)] = n_new
                else:
                    row[str(m)] = self.get_retention_count(cohort, m, cohort_data)
            
            row['Total Customers That Month'] = int(total_per_month.get(cohort, 0))
            matrix_rows.append(row)
        
        matrix_df = pd.DataFrame(matrix_rows)
        
        # Percentage matrix
        pct_rows = []
        for _, row in matrix_df.iterrows():
            n_new = row['New Customers']
            pct_row = {
                'Activation Month': row['Activation Month'],
                'New Customers': n_new,
            }
            
            for m in all_months:
                val = row[str(m)]
                if val == '':
                    pct_row[str(m)] = ''
                elif n_new == 0:
                    pct_row[str(m)] = 'N/A'
                else:
                    pct_row[str(m)] = f"{round(val / n_new * 100, 1)}%"
            
            pct_row['Total Customers That Month'] = row['Total Customers That Month']
            pct_rows.append(pct_row)
        
        pct_df = pd.DataFrame(pct_rows)
        
        return matrix_df, pct_df, all_months
    
    def build_flat_view(self, all_months, cohort_sizes, total_per_month, cohort_data):
        """Build flat view with M+1, M+2, etc."""
        logger.info("Building flat view...")
        
        flat_rows = []
        for cohort in all_months:
            n_new = int(cohort_sizes.get(cohort, 0))
            subsequent = [m for m in all_months if m > cohort]
            
            flat_row = {
                'Activation Month': str(cohort),
                'New Customers': n_new,
                'Total Customers That Month': int(total_per_month.get(cohort, 0)),
            }
            
            for i, m in enumerate(subsequent, start=1):
                ret = self.get_retention_count(cohort, m, cohort_data)
                pct = round(ret / n_new * 100, 1) if n_new > 0 else 0.0
                flat_row[f'M+{i} ({m}) Count'] = ret
                flat_row[f'M+{i} ({m}) Retain%'] = f"{pct}%"
            
            flat_rows.append(flat_row)
        
        return pd.DataFrame(flat_rows)
    
    def build_summary(self, all_months, cohort_sizes, total_per_month, cohort_data):
        """Build cohort health summary"""
        logger.info("Building summary...")
        
        summary_rows = []
        for cohort in all_months:
            n_new = int(cohort_sizes.get(cohort, 0))
            n_total = int(total_per_month.get(cohort, 0))
            
            if n_new == 0:
                continue
            
            subsequent = [m for m in all_months if m > cohort]
            
            if not subsequent:
                avg_ret = None
                n_obs = 0
            else:
                pcts = [
                    self.get_retention_count(cohort, m, cohort_data) / n_new * 100
                    for m in subsequent
                ]
                avg_ret = round(np.mean(pcts), 1)
                n_obs = len(subsequent)
            
            summary_rows.append({
                'Activation Month': str(cohort),
                'New Customers': n_new,
                'Total Customers That Month': n_total,
                'Months Observed': n_obs,
                'Avg Retention %': f"{avg_ret}%" if avg_ret is not None else '–',
            })
        
        return pd.DataFrame(summary_rows)
    
    def dataframe_to_sheets_format(self, df):
        """Convert DataFrame to list of lists for Google Sheets"""
        # Replace NaN with empty string and convert non-serializable values to string
        result = [df.columns.tolist()]
        for _, row in df.iterrows():
            sanitized = []
            for cell in row.tolist():
                # pandas NA / numpy NaN
                try:
                    if pd.isna(cell):
                        sanitized.append("")
                        continue
                except Exception:
                    # pd.isna may fail for some custom types; fall through
                    pass

                # Periods, numpy types, etc. — convert to str
                if isinstance(cell, (pd.Period,)):
                    sanitized.append(str(cell))
                else:
                    # Keep ints and strings as-is, otherwise str()
                    if isinstance(cell, (int, str)):
                        sanitized.append(cell)
                    else:
                        sanitized.append(str(cell))

            result.append(sanitized)
        return result
    
    def write_results_to_sheets(self, output_sheet_id, matrix_df, pct_df, flat_df, summary_df):
        """Write results to Google Sheets, creating/clearing sheets as needed"""
        try:
            logger.info(f"Opening output sheet: {output_sheet_id}")
            worksheet = open_spreadsheet(self.gc, output_sheet_id)

            sheets_to_create = {
                'Counts Matrix': matrix_df,
                'Retention % Matrix': pct_df,
                'Flat View': flat_df,
                'Summary': summary_df
            }

            # Ensure sheets exist (create if missing) and then write with retries
            for sheet_name, df in sheets_to_create.items():
                logger.info(f"Ensuring sheet exists: {sheet_name}")
                rows = max(10, len(df) + 5) if hasattr(df, '__len__') else 1000
                cols = max(5, len(df.columns)) if hasattr(df, 'columns') else 10
                ensure_worksheet(worksheet, sheet_name, rows=rows, cols=cols)

            # Write data to sheets with robust retry/backoff
            for sheet_name, df in sheets_to_create.items():
                logger.info(f"Writing to sheet: {sheet_name}")
                sheet = ensure_worksheet(worksheet, sheet_name, rows=max(10, len(df) + 5), cols=max(5, len(df.columns)))
                clear_worksheet(sheet)
                data = self.dataframe_to_sheets_format(df)
                append_rows(sheet, data, value_input_option='USER_ENTERED')
            
            logger.info("✅ Results successfully written to Google Sheets")
            
        except Exception as e:
            logger.error(f"Error writing to Google Sheets: {e}")
            raise
    
    def run(self, output_sheet_id):
        """Execute the complete analysis pipeline"""
        try:
            logger.info("="*80)
            logger.info("Starting Monthly Cohort Retention Analysis")
            logger.info("="*80)
            
            # Load and clean
            df = self.load_data_from_sheets()
            orders = self.clean_and_prepare(df)
            
            # Create identifiers
            identified = self.create_customer_identifier(orders)
            
            # Build aggregates
            all_months, cohort_sizes, total_per_month, cohort_data, identified = (
                self.build_monthly_aggregates(identified)
            )
            
            # Build outputs
            matrix_df, pct_df, _ = self.build_matrices(
                all_months, cohort_sizes, total_per_month, cohort_data
            )
            flat_df = self.build_flat_view(
                all_months, cohort_sizes, total_per_month, cohort_data
            )
            summary_df = self.build_summary(
                all_months, cohort_sizes, total_per_month, cohort_data
            )
            
            # Log results
            logger.info("\n" + "="*80)
            logger.info("COHORT RETENTION — SUMMARY")
            logger.info("="*80)
            logger.info("\n" + summary_df.to_string(index=False))
            
            # Write to output sheet
            self.write_results_to_sheets(
                output_sheet_id, matrix_df, pct_df, flat_df, summary_df
            )
            
            logger.info("="*80)
            logger.info("✅ Analysis Complete!")
            logger.info("="*80)
            
            return True
            
        except Exception as e:
            logger.error(f"Analysis failed: {e}", exc_info=True)
            return False


if __name__ == "__main__":
    import sys
    
    credentials_file = sys.argv[1] if len(sys.argv) > 1 else "credentials.json"
    input_sheet_id = sys.argv[2] if len(sys.argv) > 2 else None
    output_sheet_id = sys.argv[3] if len(sys.argv) > 3 else None
    
    if not (input_sheet_id and output_sheet_id):
        logger.error("Usage: python cohort_analysis.py <credentials_file> <input_sheet_id> <output_sheet_id>")
        sys.exit(1)
    
    analysis = CohortAnalysis(credentials_file, input_sheet_id)
    success = analysis.run(output_sheet_id)
    sys.exit(0 if success else 1)
