# ⚽ Transfermarkt Data Pipeline & Analytics

An end-to-end data engineering pipeline that extracts, processes, and visualizes footballer transfer and match data from 2012 to the present. 

This project demonstrates a modern data stack implementation, pulling raw football data, orchestrating transformations, and serving insights through an interactive dashboard.

## 🏗 Architecture & Tech Stack

- **Orchestration:** Apache Airflow (Containerized via Docker)
- **Infrastructure as Code (IaC):** Terraform
- **Cloud Provider:** Google Cloud Platform (GCP)
- **Data Warehouse:** BigQuery (`transfermarkt_dwh`)
- **Data Transformation:** dbt (Data Build Tool)
- **Frontend / Analytics:** Streamlit & Plotly

## ✨ Dashboard Features

The Streamlit application (`app/`) queries the BigQuery fact tables (e.g., `fact_player_match_stats`) dynamically to provide:
- **Total Offensive Firepower:** Stacked bar charts comparing club-level goals and assists.
- **Player Efficiency:** Interactive bubble charts mapping impact (goal contributions) vs. time on the pitch.
- **Scoring Volatility & Anomaly Detection:** Time-series analysis utilizing 14-day rolling averages and 95% confidence intervals to identify statistical scoring anomalies.

## 📂 Repository Structure

```text
📦 Transfermarkt-pipeline
 ┣ 📂 .github/workflows   # CI/CD pipelines
 ┣ 📂 airflow             # DAGs, plugins, and Airflow Docker configuration
 ┣ 📂 app                 # Streamlit dashboard application
 ┣ 📂 secrets             # Local directory for GCP service account keys (git-ignored)
 ┣ 📂 terraform           # HCL scripts to provision GCP GCS buckets and BigQuery datasets
 ┣ 📜 Makefile            # Automation commands for easy setup
 ┗ 📜 README.md
```

## 🚀 How to Run the Project

### 1. Prerequisites
Ensure you have the following installed on your local machine:
* Docker & Docker Compose
* Terraform
* Google Cloud CLI
* Python 3.9+
* `make` utility

```bash
git clone https://github.com/Bank7656/Transfermarkt-pipeline
cd Transfermarkt-pipeline
cp .env.example .env
echo -e "AIRFLOW_UID=$(id -u)" >> .env 
```
Open .env and fill the setup variable
* GCP_PROJECT_ID — your GCP project ID
* GCP_BUCKET_NAME — your GCP Bucket name
* BQ_DATASET - Bigquery dataset name
* KAGGLE_USERNAME / KAGGLE_KEY — from kaggle.com > Account > Create New Token (download kaggle.json, copy the username and key values)

### 2. GCP Setup & Authentication
1. Create a Google Cloud Project.
2. Create a Service Account with the following roles:
   * BigQuery Admin
   * Storage Admin
3. Generate a JSON key for the Service Account.
4. Save the key as `credentials.json` inside the `secrets/` directory.

### 3. Provision Infrastructure (Terraform)
Navigate to the `terraform` directory and initialize the GCP resources (GCS bucket and BigQuery datasets):

```bash
# Create GCP Resource
make infra-up

# Stop and destroy GCP Resource
make stop
```

*Note: You may need to update the `variables.tf` or `terraform.tfvars` file with your specific GCP Project ID.*

### 4. Start the Pipeline (Airflow)
Return to the root directory and use the provided `Makefile` to spin up the Airflow environment:

```bash
# Build and start the Airflow containers
make start

# (Optional) To tear down the environment later:
make stop

# To delete all container
make fclean
```

Once healthy, access the Airflow UI at `http://localhost:8080`. Enable the DAGs to begin fetching and processing the Transfermarkt data.

### 5. Run the Analytics Dashboard
Once the data is successfully loaded into the BigQuery `transfermarkt_dwh` dataset, you can spin up the Streamlit app.
Navigate to the `app/` directory, install the requirements, and run the app:

```bash
cd app
pip install -r requirements.txt
streamlit run app.py
```

The dashboard will be available at `http://localhost:8501`. By default, it filters to the Premier League / other league to provide an immediate look at top-flight club and player performance.