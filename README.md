# Customer Churn Analytics Warehouse using Amazon Redshift

----
## Project Overview
- This project demonstrates building a cloud-based data warehouse using Amazon Redshift to analyze telecom customer churn.
- The pipeline ingests raw CSV data from Amazon S3, loads it into Redshift using the COPY command, and performs analytical queries to generate business insights.
- It follows a typical data engineering workflow: data ingestion → staging → transformation → analytics.

---
## Dataset
```
Source: Kaggle
Link: https://www.kaggle.com/datasets/shilongzhuang/telecom-customer-churn-by-maven-analytics

Files Used:
1. telecom_customer_churn.csv - Contains customer demographics, services, tenure, and churn details.
2. zip_code_population.csv - Contains population data for zip codes.
```

---
## Technology and Services used
- Cloud Platform: AWS
- AWS Services:
    - Amazon EC2 - Linux Instance
    - Amazon Redshift – Data warehouse
    - AWS IAM – Role-based access to S3
- Programming Language: Python 3
- Libraries : Boto3

---
## Data Flow:
```
Local System
    ↓
Upload CSV Files
    ↓
Amazon S3 (Raw Data)
    ↓
Amazon Redshift (Staging Tables)
    ↓
COPY Command (Data Load)
    ↓
Analytical Table (churn_analytics)
    ↓
SQL Queries (Business Insights)
```

---
## Installation
```
-> Install this system level ODBC driver(for linux systems):
sudo yum install gcc gcc-c++ python3-devel unixODBC-devel -y

Clone the repository
-> git clone https://github.com/Parashuram-V-Pawar/customer_churn_nalytics_warehouse_redshift.git

Move to project folder
-> cd customer_churn_nalytics_warehouse_redshift

Create a virtual environment inside it
python3 -m venv venv
source venv/bin/activate

Install dependencies
-> pip install -r requirements.txt

Run the application
```

## Author
```
Parashuram V Pawar
GitHub username: Parashuram-V-Pawar
```