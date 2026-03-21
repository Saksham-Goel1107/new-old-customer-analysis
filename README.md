# Monthly Customer Retention Cohort Analysis - Docker Deployment

Automated cohort analysis that reads from Google Sheets, performs retention calculations, and writes results back to Google Sheets. Runs daily at 7:00 AM UTC.

## Features

✅ **Automated Daily Execution** - Runs at 7:00 AM UTC every day  
✅ **Google Sheets Integration** - Reads from and writes back to Google Sheets  
✅ **Production-Ready** - Complete with logging, error handling, health checks  
✅ **Docker Containerized** - Easy deployment on any server  
✅ **Accurate Analytics** - 4 detailed output sheets  

## Prerequisites

- Docker & Docker Compose installed
- Google Cloud Project with Sheets API enabled
- Service Account credentials file
- A Google Sheet with sales data
- An output Google Sheet (can be the same)

## Setup Instructions

### 1. Create Google Cloud Service Account

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a new project (or use existing)
3. Enable the **Google Sheets API**:
   - Click "APIs & Services" → "Library"
   - Search for "Google Sheets API"
   - Click → "Enable"
4. Create a **Service Account**:
   - Go to "APIs & Services" → "Credentials"
   - Click "Create Credentials" → "Service Account"
   - Fill in details (name, email auto-generated)
   - Click "Create and Continue"
   - **Grant Editor access** to the service account
   - Click "Continue" then "Done"
5. Get credentials JSON:
   - Click on the service account email
   - Go to "Keys" tab
   - Click "Add Key" → "Create new key"
   - Choose "JSON"
   - Download (saves automatically)
   - Rename to `credentials.json`

### 2. Share Google Sheets with Service Account

1. Open your **Input Sheet** (with sales data) and **Output Sheet**
2. Click Share button
3. Add the service account email (from step 1, looks like: `xxx@xxx.iam.gserviceaccount.com`)
4. Give **Editor** permissions
5. Repeat for both sheets

### 3. Prepare Input Data Sheet

Your input Google Sheet must have these columns (case-sensitive):
```
date              → Date of order (e.g., "2025-11-18")
number            → Bill/Order number (unique identifier)
customerMobile    → Customer phone number (primary identifier)
customerName      → Customer name (fallback identifier)
orderAmount       → Order total amount
```

### 4. Configure Environment

Clone this repository and create `.env` file:

```bash
cp .env.example .env
```

Edit `.env` with your values:

```env
# Find these from your Google Sheet URLs
# URL format: https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit
INPUT_SHEET_ID=paste-your-input-sheet-id
OUTPUT_SHEET_ID=paste-your-output-sheet-id

# Path to your downloaded credentials
GOOGLE_CREDENTIALS_FILE=/app/credentials.json

# Run daily at 7am UTC
RUN_MODE=scheduled
```

### 5. Copy Credentials File

Place your `credentials.json` in the project root:

```bash
cp ~/Downloads/credentials.json ./credentials.json
```

### 6. Deploy with Docker

**Option A: Docker Compose (Recommended)**

```bash
# Start the container in background
docker-compose up -d

# View logs
docker-compose logs -f cohort-analysis

# Stop the container
docker-compose down
```

**Option B: Docker CLI**

```bash
# Build image
docker build -t cohort-analysis .

# Run container
docker run -d \
  --name cohort-analysis \
  -e INPUT_SHEET_ID=your-input-id \
  -e OUTPUT_SHEET_ID=your-output-id \
  -v $(pwd)/credentials.json:/app/credentials.json:ro \
  -v $(pwd)/logs:/app/logs \
  cohort-analysis
```

## Usage

### Automatic Execution
Once running, the analysis automatically executes **every day at 7:00 AM UTC**.

### Manual Execution (One-time)
```bash
# Using docker-compose
docker-compose run --profile manual cohort-analysis-once

# Using docker CLI
docker run --rm \
  -e RUN_MODE=once \
  -e INPUT_SHEET_ID=your-input-id \
  -e OUTPUT_SHEET_ID=your-output-id \
  -v $(pwd)/credentials.json:/app/credentials.json:ro \
  cohort-analysis
```

### View Logs
```bash
# Docker Compose
docker-compose logs -f cohort-analysis

# Docker CLI
docker logs -f cohort-analysis
```

## Output Sheets

The analysis creates/updates **4 sheets** in your output Google Sheet:

### 1. **Counts Matrix**
Raw customer counts showing retention by cohort
- Rows = Cohorts (month of first purchase)
- Columns = Months (each month's returning customers)
- Last column = Total customers in that month

### 2. **Retention % Matrix**
Same as Counts Matrix but showing percentages
- Helps identify cohort health at a glance
- Calculated as: (Returning Customers / New Customers) × 100

### 3. **Flat View**
Month-over-month view showing M+1, M+2, M+3, etc.
- One row per cohort
- Shows retention 1 month after, 2 months after, etc.
- Includes both counts and percentages

### 4. **Summary**
Cohort health snapshot
- New Customers = Customers acquired in that month
- Total Customers That Month = All unique buyers (new + returning)
- Months Observed = Number of months tracked post-acquisition
- Avg Retention % = Average retention across all observed months

## Configuration

### Change Execution Time

Edit `app.py` line ~50, modify the `CronTrigger`:

```python
# Change hour to desired UTC hour
scheduler.add_job(
    func=run_analysis,
    trigger=CronTrigger(hour=7, minute=0, timezone='UTC'),  # ← Change hour here
    ...
)
```

Rebuild and redeploy:
```bash
docker-compose down
docker-compose up -d --build
```

### Change Input Sheet Name

Default sheet name is `"Sales Data"`. To change in `cohort_analysis.py`:

```python
analysis = CohortAnalysis(
    credentials_file,
    input_sheet_id,
    input_sheet_name="Your Sheet Name"  # ← Change here
)
```

## Troubleshooting

### Error: "Sheet not found"
- Verify sheet name exists in Google Sheet
- Check you've shared the sheet with service account email

### Error: "Permission denied"
- Ensure service account has **Editor** access to both sheets
- Check credentials.json is valid and in correct location

### Container won't start
```bash
# Check logs
docker-compose logs cohort-analysis

# Verify environment variables
docker-compose config
```

### Analysis runs but produces no output
- Check if analysis produces errors in logs
- Verify credentials.json file is readable
- Ensure input sheet has correct column names (case-sensitive)

## Data Schema - Input Requirements

Required columns in input Google Sheet:

| Column | Type | Example | Notes |
|--------|------|---------|-------|
| date | Date | 2025-11-18 | Order date |
| number | String | INV001 | Unique bill/order ID |
| customerMobile | String | 923001234567 | Phone number (primary ID) |
| customerName | String | John Doe | Customer name (fallback ID) |
| orderAmount | Number | 5000 | Order total |

### Customer Identification Logic
1. **Priority 1**: Use `customerMobile` if present and valid (non-empty, not "0", not "nan")
2. **Priority 2**: Use `customerName` if mobile unavailable
3. **Priority 3**: Exclude order if neither mobile nor name exist

This ensures accurate retention tracking even with incomplete data.

## Performance Notes

- **Data Size**: Handles 10K+ orders efficiently
- **Memory**: Typically uses 256-512MB
- **Execution Time**: Usually completes in 5-30 seconds depending on data size
- **CPU**: Low CPU usage during execution

## Security Best Practices

✅ Never commit `credentials.json` to version control  
✅ Use `.gitignore` to exclude sensitive files  
✅ Don't share service account credentials  
✅ Regularly rotate service account keys  
✅ Use read-only credentials for output if possible  

## Deployment on Server

### Linux Server (Ubuntu/Debian)

```bash
# Install Docker & Compose
sudo apt-get update
sudo apt-get install docker.io docker-compose

# Clone/copy project
cd /opt/cohort-analysis

# Setup
cp .env.example .env
# Edit .env with your values
vim .env
cp credentials.json ./credentials.json

# Deploy
docker-compose up -d

# Verify
docker-compose logs -f
```

### Advanced: Using systemd

Create `/etc/systemd/system/cohort-analysis.service`:

```ini
[Unit]
Description=Cohort Analysis Service
After=docker.service
Requires=docker.service

[Service]
WorkingDirectory=/opt/cohort-analysis
ExecStart=/usr/bin/docker-compose up
ExecStop=/usr/bin/docker-compose down
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

Then:
```bash
sudo systemctl daemon-reload
sudo systemctl enable cohort-analysis
sudo systemctl start cohort-analysis
sudo systemctl status cohort-analysis
```

## Monitoring

```bash
# Check memory usage
docker stats cohort-analysis

# View container details
docker inspect cohort-analysis

# Check last execution time
docker-compose logs cohort-analysis | tail -20
```

## Support & Issues

For issues:
1. Check logs: `docker-compose logs cohort-analysis`
2. Verify credentials and permissions
3. Ensure input sheet has correct schema
4. Re-run with `RUN_MODE=once` for immediate testing

## Updates & Maintenance

```bash
# Pull latest changes
git pull

# Rebuild container
docker-compose up -d --build

# Clean up old images
docker image prune
```

## License

This project is provided as-is for analysis purposes.

---

**Ready to deploy?** Follow the setup steps above and run `docker-compose up -d` 🚀
