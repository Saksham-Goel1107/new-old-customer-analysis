"""
Total Customer Base Analysis
Reads sales data from Google Sheets and writes 5 output sheets:
 - Monthly Composition
 - MoM Retention (from total base)
 - Matrix Counts (total base)
 - Matrix % (total base)
 - Combined Summary

Usage:
    python total_analysis.py <credentials_file> <input_sheet_id> <output_sheet_id> [input_sheet_name]

"""
import pandas as pd
import numpy as np
from datetime import datetime
import logging
from google.oauth2.service_account import Credentials
import gspread
from sheets_utils import open_spreadsheet, ensure_worksheet, clear_worksheet, append_rows

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

INVALID = {"nan", "", "none", "0", "0.0"}


def dataframe_to_sheets_format(df: pd.DataFrame):
    result = [df.columns.tolist()]
    for _, row in df.iterrows():
        sanitized = []
        for cell in row.tolist():
            try:
                if pd.isna(cell):
                    sanitized.append("")
                    continue
            except Exception:
                pass
            if isinstance(cell, (pd.Period,)):
                sanitized.append(str(cell))
            else:
                if isinstance(cell, (int, str)):
                    sanitized.append(cell)
                else:
                    sanitized.append(str(cell))
        result.append(sanitized)
    return result


class TotalCustomerAnalysis:
    def __init__(self, credentials_file, input_sheet_id, input_sheet_name='main'):
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
        creds = Credentials.from_service_account_file(credentials_file, scopes=scope)
        self.gc = gspread.authorize(creds)
        self.input_sheet_id = input_sheet_id
        self.input_sheet_name = input_sheet_name

    def load_data_from_sheets(self):
        logger.info("Opening input sheet: %s", self.input_sheet_id)
        sh = open_spreadsheet(self.gc, self.input_sheet_id)
        try:
            ws = sh.worksheet(self.input_sheet_name)
        except Exception as e:
            from gspread.exceptions import WorksheetNotFound
            if isinstance(e, WorksheetNotFound):
                titles = [w.title for w in sh.worksheets()]
                logger.warning("Worksheet '%s' not found. Available sheets: %s. Falling back to first sheet.", self.input_sheet_name, titles)
                if not titles:
                    raise
                ws = sh.worksheet(titles[0])
            else:
                raise

        data = ws.get_all_records()
        logger.info("Loaded %d rows", len(data))
        df = pd.DataFrame(data)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce', utc=True)
            df['date'] = df['date'].dt.tz_localize(None)
        return df

    def clean_prepare(self, df: pd.DataFrame):
        df = df[df['date'].notna() & df['number'].notna() & (df['number'].astype(str).str.strip() != 'Total')].copy()
        orders = df.drop_duplicates(subset='number').copy()
        orders = orders[["number", "date", "customerMobile", "customerName", "orderAmount"]].copy()
        orders['customerMobile'] = orders['customerMobile'].astype(str).str.strip().str.lower()
        orders['customerName'] = orders['customerName'].astype(str).str.strip().str.lower()
        orders['customer_id'] = orders.apply(self.resolve_customer_id, axis=1)
        identified = orders[orders['customer_id'].notna()].copy()
        return identified

    def resolve_customer_id(self, row):
        mob = row['customerMobile']
        name = row['customerName']
        if mob not in INVALID:
            return f"MOB:{mob}"
        elif name not in INVALID:
            return f"NAME:{name}"
        return None

    def build_outputs(self, identified: pd.DataFrame):
        identified['month'] = identified['date'].dt.to_period('M')
        data_months = sorted(identified['month'].dropna().unique())
        today_period = pd.Period(datetime.today(), freq='M')
        start = data_months[0]
        end = max(data_months[-1], today_period)
        all_months = list(pd.period_range(start=start, end=end, freq='M'))

        first_purchase = identified.groupby('customer_id')['month'].min().rename('cohort_month')
        identified = identified.join(first_purchase, on='customer_id')
        identified['is_new'] = identified['cohort_month'] == identified['month']

        # monthly sets
        monthly_customers = {m: set(identified[identified['month'] == m]['customer_id']) for m in all_months}

        # Sheet 1 - composition
        new_per_month = identified[identified['is_new']].groupby('month')['customer_id'].nunique()
        total_per_month = identified.groupby('month')['customer_id'].nunique()
        ret_per_month = identified[~identified['is_new']].groupby('month')['customer_id'].nunique()

        comp_rows = []
        for m in all_months:
            total = int(total_per_month.get(m, 0))
            new_c = int(new_per_month.get(m, 0))
            ret_c = int(ret_per_month.get(m, 0))
            new_pct = round(new_c / total * 100, 1) if total > 0 else 0.0
            ret_pct = round(ret_c / total * 100, 1) if total > 0 else 0.0
            comp_rows.append({
                'Month': str(m),
                'Total Customers': total,
                'New Customers': new_c,
                'Returning Customers': ret_c,
                'New % of Total': f"{new_pct}%",
                'Returning % of Total': f"{ret_pct}%",
            })
        comp_df = pd.DataFrame(comp_rows)

        # Sheet 2 - MoM from total base
        mom_rows = []
        for i, m in enumerate(all_months):
            total = len(monthly_customers[m])
            row = {'Base Month': str(m), 'Total Customers': total}
            subsequent = all_months[i+1:]
            for j, m_next in enumerate(subsequent, start=1):
                overlap = len(monthly_customers[m] & monthly_customers[m_next])
                pct = round(overlap / total * 100, 1) if total > 0 else 0.0
                row[f"M+{j} ({m_next}) Came Back"] = overlap
                row[f"M+{j} ({m_next}) Retain%"] = f"{pct}%"
            mom_rows.append(row)
        mom_df = pd.DataFrame(mom_rows)

        # Sheet 3 - matrix counts & %
        matrix_rows = []
        for m_base in all_months:
            total = len(monthly_customers[m_base])
            row = {'Base Month': str(m_base), 'Total Customers': total}
            for m_col in all_months:
                if m_col < m_base:
                    row[str(m_col)] = ''
                elif m_col == m_base:
                    row[str(m_col)] = total
                else:
                    overlap = len(monthly_customers[m_base] & monthly_customers[m_col])
                    row[str(m_col)] = overlap
            matrix_rows.append(row)
        matrix_df = pd.DataFrame(matrix_rows)

        pct_rows = []
        for _, row in matrix_df.iterrows():
            total = row['Total Customers']
            pct_row = {'Base Month': row['Base Month'], 'Total Customers': total}
            for m_col in all_months:
                val = row[str(m_col)]
                if val == '':
                    pct_row[str(m_col)] = ''
                elif total == 0:
                    pct_row[str(m_col)] = 'N/A'
                else:
                    pct_row[str(m_col)] = f"{round(val / total * 100, 1)}%"
            pct_rows.append(pct_row)
        pct_df = pd.DataFrame(pct_rows)

        # Sheet 4 - summary
        cum_customers = set()
        summary_rows = []
        for i, m in enumerate(all_months):
            total = len(monthly_customers[m])
            new_c = int(new_per_month.get(m, 0))
            ret_c = int(ret_per_month.get(m, 0))
            cum_customers |= monthly_customers[m]

            if i < len(all_months) - 1:
                m_next = all_months[i+1]
                overlap = len(monthly_customers[m] & monthly_customers[m_next])
                mom_ret = f"{round(overlap / total * 100, 1)}%" if total > 0 else '–'
                mom_count = overlap
            else:
                mom_ret = '–'
                mom_count = 0
            summary_rows.append({
                'Month': str(m),
                'Total Customers': total,
                'New Customers': new_c,
                'Returning Customers': ret_c,
                'New % of Total': f"{round(new_c/total*100,1)}%" if total > 0 else '–',
                'Returning % of Total': f"{round(ret_c/total*100,1)}%" if total > 0 else '–',
                'Came Back Next Month (Count)': mom_count if i < len(all_months)-1 else '–',
                'Came Back Next Month %': mom_ret,
                'Cumulative Unique Customers': len(cum_customers),
            })
        summary_df = pd.DataFrame(summary_rows)

        return comp_df, mom_df, matrix_df, pct_df, summary_df

    def write_results_to_sheets(self, output_sheet_id, comp_df, mom_df, matrix_df, pct_df, summary_df):
        logger.info('Opening output sheet: %s', output_sheet_id)
        out = open_spreadsheet(self.gc, output_sheet_id)
        sheets_to_write = {
            'Monthly Composition': comp_df,
            'MoM Retention': mom_df,
            'Matrix Counts': matrix_df,
            'Matrix %': pct_df,
            'Combined Summary': summary_df,
        }

        # Ensure sheets exist
        for name, df in sheets_to_write.items():
            logger.info('Ensuring sheet exists: %s', name)
            rows = max(10, len(df) + 5) if hasattr(df, '__len__') else 1000
            cols = max(5, len(df.columns)) if hasattr(df, 'columns') else 10
            ensure_worksheet(out, name, rows=rows, cols=cols)

        # Write with retry/backoff
        for name, df in sheets_to_write.items():
            logger.info('Writing to sheet: %s', name)
            ws = ensure_worksheet(out, name, rows=max(10, len(df) + 5), cols=max(5, len(df.columns)))
            clear_worksheet(ws)
            data = dataframe_to_sheets_format(df)
            append_rows(ws, data, value_input_option='USER_ENTERED')
        logger.info('✅ Results written to output sheet')

    def run(self, output_sheet_id):
        try:
            df = self.load_data_from_sheets()
            identified = self.clean_prepare(df)
            comp_df, mom_df, matrix_df, pct_df, summary_df = self.build_outputs(identified)
            self.write_results_to_sheets(output_sheet_id, comp_df, mom_df, matrix_df, pct_df, summary_df)
            return True
        except Exception as e:
            logger.exception('Analysis failed: %s', e)
            return False


if __name__ == '__main__':
    import sys
    creds = sys.argv[1] if len(sys.argv) > 1 else 'credentials.json'
    in_id = sys.argv[2] if len(sys.argv) > 2 else None
    out_id = sys.argv[3] if len(sys.argv) > 3 else None
    sheet_name = sys.argv[4] if len(sys.argv) > 4 else 'Sales Data'
    if not (in_id and out_id):
        print('Usage: python total_analysis.py <credentials_file> <input_sheet_id> <output_sheet_id> [input_sheet_name]')
        sys.exit(1)
    ta = TotalCustomerAnalysis(creds, in_id, sheet_name)
    ok = ta.run(out_id)
    sys.exit(0 if ok else 1)
