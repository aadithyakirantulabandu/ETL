Project Explanation — Real-Time Healthcare ETL, De-Identification, QC, and Power BI
1) ## Purpose and Problem Statement
 Doing this with real PHI is difficult due to privacy, legal, and access constraints. This project provides a production-style, local-first pipeline that simulates that environment safely:
•	Continuously ingests raw patient events.
•	Cleans and standardizes messy fields.
•	De-identifies per HIPAA Safe Harbor so only masked data leaves the machine.
•	Runs quality control (QC) and outlier detection to catch bad data early.
•	Publishes a live, analytics-ready dataset to files or a database for dashboards.
The project demonstrates the full lifecycle from data generation → ingestion → processing → analytics, with practical choices you would make in a real hospital analytics stack.

2) ## Why Synthea and How It Mirrors Real-World Data
Synthea is an open-source synthetic patient generator that produces realistic clinical journeys (encounters, vitals, labs, diagnoses, medications) without any real PHI. It is used here for three reasons:
1.	Realism of longitudinal care: Records include multi-year histories and common clinical patterns (e.g., hypertension, diabetes), similar to what you would see in EHR systems.
2.	Schema complexity: Multiple related tables (patients, encounters, observations) resemble typical EHR exports (HL7/FHIR/CSV), forcing the same mapping and joining steps you do in production.
3.	Operational safety: Because all data is synthetic, it can be generated, shared, and processed freely during development and demos.
To better reflect real-world data quality issues, the pipeline intentionally injects noise (missing values, typos, occasional extreme values). This simulates manual entry mistakes, device glitches, unit inconsistencies, and integration gaps that are common in hospital data warehouses.

3) ## End-to-End Flow
1.	Synthetic generation (Synthea + generator script):
Synthea produces batches of CSV output (vitals, demographics, observations). A generator script runs it repeatedly to mimic a feed of new patients and encounters arriving over time.
2.	Noise injection:
A small module modifies a portion of the rows to introduce realistic imperfections (nulls, swapped digits, jitter). This step stress-tests the downstream cleaning and QC logic.
3.	Mapping to a unified “events” format:
Multiple Synthea CSVs are transformed into a single standardized events CSV (events_*.csv) with the columns required by the pipeline (e.g., patient_id, dob, event_ts, systolic_bp, diastolic_bp, heart_rate, zip).
4.	Drop into ingress folder:
The mapped file is written into incoming/. This folder plays the role of a landing zone or message queue in a micro-batch system.
5.	Watcher loop triggers processing:
A lightweight watcher monitors incoming/ and, upon seeing a new file, calls the pipeline to process it.
6.	Pipeline steps:
o	Schema enforcement: Verify required columns and coerce types (dates, numbers, strings).
o	Cleaning and standardization: Pad ZIPs, clip physiologic ranges (e.g., HR not below 20 or above 240), normalize time fields.
o	QC and outlier detection: Use IQR or MAD to flag improbable values; optionally quarantine batches with excessive anomalies.
o	De-identification: Apply HIPAA Safe Harbor rules:
i.  Remove direct identifiers (first/last name).
ii.	Convert patient_id to a salted HMAC hash (patient_key) for linkage without re-identification.
iii.Generalize dates (e.g., DOB→year only, event timestamp→date only).
iv.	Truncate ZIP to ZIP3 (first three digits).
o	Sinks: Append masked rows to Parquet and/or SQLite; optionally push rows to a Power BI streaming dataset.
7.	Analytics:
Power BI connects to the masked dataset (Parquet or SQLite) to render KPIs, time trends, distributions, and QC monitoring in near-real time.

4) ## Data Model (Analytics-Ready “CleanEvent”)
After processing and de-identification, the analytics table contains:
•	patient_key — HMAC-SHA256 hash of the original ID using a secret salt to preserve linkage without exposing identities.
•	dob_year — Year of birth (DOB generalized).
•	event_date — Calendar date of event (time removed).
•	zip3 — First three digits of ZIP (geographic generalization).
•	systolic_bp, diastolic_bp, heart_rate — Cleaned vital signs coerced to numeric within physiologic limits.
•	outlier_flags — Comma-separated indicators (e.g., flag_systolic_bp) if values were detected outside expected distributions.
This minimal schema is intentionally “tall and tidy” so that BI tools can aggregate quickly by time, age, and region.

5) ## Cleaning and Quality Control
Cleaning addresses systematic, predictable issues:
•	Type coercion: Convert free-text or mixed columns to proper types (date, numeric).
•	ZIP normalization: Pad to five digits, then derive zip3.
•	Physiologic clipping: Keep values within medically plausible ranges to reduce downstream skew.
QC and Outliers catch atypical or erroneous values:
•	IQR (interquartile range) flags points beyond typical variability within a batch.
•	MAD (median absolute deviation) offers robustness to heavy-tailed distributions.
•	Action: Either flag rows so analysts can filter them in dashboards, or quarantine a file to prevent polluting aggregates.
This mirrors hospital data governance where suspect feeds are either marked for review or blocked until corrected.

6) ## De-Identification Strategy
The pipeline follows HIPAA Safe Harbor-style generalization and removal:
•	Direct identifiers removed (first/last name).
•	HMAC of patient_id with a secret salt produces a stable, unlinkable patient_key. This supports longitudinal analysis without re-identification risk.
•	Date generalization: DOB reduced to year; event timestamp reduced to date only (no time).
•	Geography generalization: ZIP lowered to ZIP3 to reduce spatial precision.
All raw/PHI processing occurs locally; only the masked table is written to shared sinks.

7) ## Orchestration, Resilience, and Observability
•	File watcher simulates a streaming system using micro-batches. The loop is simple, transparent, and easy to debug.
•	Quarantine folder receives files that fail schema validation or exceed configured QC thresholds.
•	Logging to file and console supports traceability and incident review.
•	Alerts (email/Slack) can be wired in to notify on failures, enabling basic on-call workflows.
•	Retry logic wraps transient operations (I/O, network calls) to reduce flakiness.
This is analogous to production ingestion services with dead-letter queues and alerting.

8) ## Output Sinks and Dashboarding Options
•	Parquet (Import mode): Columnar, compact, fast for batch analytics. In Power BI Desktop, refresh manually to see new rows (or use scheduled refresh in the Service).
•	SQLite (DirectQuery via ODBC): Enables automatic page refresh in Power BI Desktop for a local, near-live dashboard as the pipeline appends rows.
•	Power BI Push Dataset (Service): The pipeline can push rows into a streaming dataset for seconds-level updates in the cloud.
These cover the common operating modes: local development, local live monitoring, and cloud dashboards.

9) ## Real-Time Behavior
The system uses micro-batch streaming:
•	Synthea emits a batch; the watcher detects a new file within seconds and processes it.
•	Typical end-to-end latency is dominated by batch size, file I/O, and dashboard refresh mode:
o	SQLite + DirectQuery + page refresh: near-live updates on screen.
o	Parquet + Import: updates appear upon manual refresh (Desktop) or scheduled refresh (Service).
o	Push dataset: seconds-level updates in Service.
For hospital-scale throughput or strict SLAs, the same structure can be ported to message queues and stream processors (e.g., Kafka + Spark/Flink) while preserving the de-ID/QC steps.

10) ## Security and Governance
•	Local-only PHI handling: Raw files and PHI exist only on the local machine.
•	Secrets in .env: HMAC salt, webhook URLs, and SMTP credentials are never committed.
•	Principle of least privilege: Sinks receive only masked fields.
•	Reproducibility: Configuration, seeds, and logs make runs auditable and repeatable.
This aligns with common privacy programs and audit expectations.

11) ## Extensibility
•	Additional domains: Add labs, medications, diagnoses by extending the mapper to include more Synthea outputs.
•	Advanced QC: Cross-field rules (e.g., diastolic must be < systolic; impossible ages), device-specific validation, site normalization.
•	Warehouses: Swap sinks to Postgres/SQL Server/BigQuery/Redshift for scalable storage and enterprise BI.
•	De-ID variants: If Safe Harbor is insufficient, incorporate Expert Determination guidelines (noise, k-anonymity, l-diversity).
•	Scheduling/ops: Replace the simple watcher with a scheduler (Airflow) or event bus for production orchestration.

12) ## Known Trade-offs and Limitations
•	Synthetic vs real: Synthea approximates clinical reality but cannot fully capture every coding quirk, integration artifact, or device idiosyncrasy found in real hospitals.
•	Micro-batching: File-based streaming is simple but not as scalable or resilient as message queues under heavy load.
•	SQLite concurrency: Good for local live dashboards, not for multi-user or high-throughput writes.
•	Parquet refresh: Import mode requires manual or scheduled refresh to reflect new data.
•	Schema coupling: Push datasets require strict schema stability; column changes mean recreating the dataset.

13) ## What This Project Demonstrates
•	A realistic, privacy-preserving data engineering pattern for healthcare analytics.
•	Robust handling of messy data via cleaning and QC with clear operator feedback (flags/quarantine).
•	Practical de-identification that preserves analytic utility while reducing risk.
•	Multiple analytics delivery modes to match local development and near-real-time monitoring needs.
•	A clean separation of concerns (generation → ingestion → processing → analytics) that can be lifted into enterprise tooling with minimal redesign.
This combination of realism, safety, and operational detail makes the project a strong template for teams preparing to connect to real EHR feeds under strict governance.



---

## System Architecture

1. **Data Generation (Synthea + generator.ps1)**  
   Generates synthetic patient records in CSV format. Runs in batches and continuously feeds new data.

   ![Generator Screenshot](docs\images\Generator.png)


2. **Noise Injection (noise_injector.py)**  
   Adds missingness, typos, and other data imperfections to mimic real-world healthcare data quality issues.

3. **Mapping (mapper_synthea_events.py)**  
   Converts raw Synthea CSVs into standardized `events_*.csv` files that match the pipeline schema.

4. **File Watcher (watcher.py)**  
   Continuously monitors the `/incoming` directory for new CSV files. Any new file is automatically processed.
   ![Watcher Screenshot](docs\images\Processing.png)


5. **Pipeline (pipeline.py)**  
   - Validates schema and cleans data.  
   - Runs QC checks and outlier detection.  
   - De-identifies PHI according to HIPAA Safe Harbor rules.  
   - Sends cleaned data to outputs: Parquet, SQLite, or Power BI.

6. **Outputs (sinks.py)**  
   Writes de-identified, cleaned data into local Parquet or SQLite. Optionally pushes rows to a Power BI streaming dataset.

7. **Dashboard (Power BI)**  
   Connects to the cleaned data. Provides KPIs, trends, distributions, and QC monitoring.

---
## Dashboard Overview

The Power BI dashboard provides a near-real-time view of patient vitals and population health indicators generated through the ETL pipeline. It demonstrates how an epidemiologist or public health analyst can quickly move from raw patient events to interpretable insights.

### 1. Trends in Vitals Over Time
- **Line chart (top left):**  
  Displays the **average systolic blood pressure, diastolic blood pressure, and heart rate by year**.  
  - Helps identify long-term trends in population-level vitals.  
  - Useful for monitoring shifts in health outcomes over decades or in response to interventions.  

### 2. Hypertension and Tachycardia by Age
- **Bar + line chart (top middle):**  
  - Blue bars show the **count of patients with hypertension** (systolic > 140 mmHg or diastolic > 90 mmHg).  
  - Dark blue line shows **tachycardia prevalence** (heart rate > 100 bpm).  
  - Stratified by **age bins**, this view highlights how chronic conditions increase with age.  

### 3. Relationship Between Vitals and Age
- **Scatter/Play axis chart (top right):**  
  - Plots the **average systolic BP vs. average heart rate**, grouped by age.  
  - The play slider at the bottom lets users explore changes across different age bins dynamically.  
  - Useful for studying the correlation between blood pressure and heart rate in subpopulations.  

### 4. Key Population Metrics
- **Cards (bottom row):**  
  - **Total Patients (33K):** Total number of individuals in the dataset.  
  - **Hypertension Rate (0.87):** Proportion of patients meeting the hypertension criteria.  
  - **Tachycardia Rate (0.23):** Proportion of patients with tachycardia.  

  ![Dashboard Screenshot](docs\images\Dashboard.png)


---


## Explanation of Each File

### Top-level files

- **generator.ps1**  
  PowerShell script to run Synthea continuously in batches. It creates synthetic patient data, adds noise, and maps it into the `/incoming` folder so the watcher can process it.

- **config.yaml**  
  Central configuration file. Defines schema requirements, cleaning rules, HIPAA de-identification settings, outlier detection thresholds, and sink options.

- **.env**  
  Stores secrets and environment variables such as de-identification salts, Power BI push URLs, and SMTP credentials. Not committed to version control.

- **requirements.txt**  
  Lists Python dependencies needed to run the ETL pipeline.

- **logs/**  
  Stores pipeline logs and error traces for auditing and troubleshooting.

- **incoming/**  
  Folder monitored by `watcher.py`. Any file dropped here is automatically processed.

- **masked_out/**  
  Contains de-identified outputs (Parquet and/or SQLite database). These are safe to use for analysis and visualization.

- **quarantine/**  
  Holds problematic files (e.g., missing schema columns, excessive outliers) that failed processing.

- **synthea_stream/**  
  Holds raw batch outputs from Synthea before mapping and injection.

---

### `app/` package

- **__init__.py**  
  Marks `app` as a Python package so modules can be imported.

- **watcher.py**  
  Continuously polls the `/incoming` folder and triggers the ETL pipeline for new files.

- **pipeline.py**  
  The main processing engine. 
  Steps:
  1. Read input file.
  2. Enforce schema.
  3. Apply cleaning rules (e.g., clipping physiologic ranges).
  4. Run QC checks and outlier detection.
  5. Apply de-identification transformations.
  6. Save results to sinks or move file to quarantine.

- **deid.py**  
  Implements HIPAA Safe Harbor de-identification:
  - Hashes patient IDs with HMAC and salt.
  - Removes direct identifiers (names).
  - Truncates ZIP to 3 digits.
  - Generalizes dates (DOB → year, event_ts → date only).

- **qc.py**  
  Quality control module:
  - Clips values to physiologic ranges.
  - Detects outliers using IQR or MAD.
  - Flags rows with suspicious data.

- **sinks.py**  
  Output handlers:
  - Write data to Parquet file.
  - Append data to SQLite table.
  - Push data to Power BI API endpoint.

- **alerts.py**  
  Optional alerting module:
  - Sends email or Slack alerts if pipeline errors occur.

- **utils.py**  
  Utility functions for:
  - Logging setup.
  - Retry logic for transient errors.
  - YAML loader.
  - HMAC hashing for de-identification.

- **schemas.py**  
  Defines data schemas using Pydantic models for validation and standardization.

- **noise_injector.py**  
  Adds artificial data issues such as missing values, random errors, and typos to mimic real-world messy data.

- **mapper_synthea_events.py**  
  Converts raw Synthea output CSVs into standardized `events_*.csv` with the expected columns.

---

## How to Run

1. **Install dependencies**
   ```powershell
   python -m venv .venv
   .\.venv\Scripts\activate
   pip install -r requirements.txt

2. **Run the watcher**
  ```powershell
    python -m app.watcher
    This will monitor the /incoming folder and process any new CSV files.
    1.Successful runs append masked rows to masked_out/cleaned.parquet and masked_out/cleaned.sqlite.
    2.Invalid files are moved to the quarantine/ folder and errors are logged in

2. **Generate synthetic data**
  ```powershell
  .\generator.ps1
### This script will:
- Generate batches of synthetic patients.  
- Export raw data to `synthea_stream/run_<timestamp>/`.  
- Apply noise injection and mapping to create a single `events_*.csv`.  
- Drop the file into `/incoming` for ETL processing.  

---

### Check processed outputs
After processing, cleaned and de-identified data will be available in:
- `masked_out/cleaned.parquet` (columnar format for analytics).  
- `masked_out/cleaned.sqlite` (SQLite database with table `cleaned_events`).  

---

### Connect Power BI

**Option A — Parquet Import**
- Power BI Desktop → **Get Data → Parquet** → load `masked_out/cleaned.parquet`.  
- Refresh manually or configure scheduled refresh in Power BI Service.  

**Option B — SQLite DirectQuery**
- Install [SQLite ODBC Driver].  
- Power BI Desktop → **Get Data → ODBC** → connect to `masked_out/cleaned.sqlite`.  
- Enable **DirectQuery + Page Refresh** for near-live dashboards.  

**Option C — Power BI Push Dataset**
- Create a streaming dataset in Power BI Service.  
- Set `sinks.powerbi_push.enabled: true` in `config.yaml`.  
- Add your dataset REST endpoint in `.env`.  

---

### Logs and Monitoring
- Logs are written to `logs/pipeline.log`.  
- Failed or quarantined files are moved to `/quarantine`.  
- Email/Slack alerts can be enabled via `.env` and `config.yaml`.  

---

### Stopping the system
- Stop the watcher with `Ctrl+C`.  
- Stop the generator script with `Ctrl+C`.  




