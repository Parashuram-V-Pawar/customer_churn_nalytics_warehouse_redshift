import logging
from scripts.query_executor import run_query

logging.basicConfig(level=logging.INFO)
logging.info("Table creation started...")
# -------------------------------------------------------------------------
# Creating and inserting values to telecom_customer_churn(Staging table)
# -------------------------------------------------------------------------
try:
    logging.info("Creating telecom_customer_churn table...")
    create_telecom_customer_churn_table = """
    CREATE TABLE IF NOT EXISTS telecom_customer_churn (
        customer_id VARCHAR(20),
        gender VARCHAR(10),
        age INT,
        married VARCHAR(5),
        number_of_dependents INT,
        city VARCHAR(100),
        zip_code VARCHAR(10),
        latitude FLOAT,
        longitude FLOAT,
        number_of_referrals INT,
        tenure_in_months INT,
        offer VARCHAR(50),
        phone_service VARCHAR(5),
        avg_monthly_long_distance_charges DECIMAL(10,2),
        multiple_lines VARCHAR(20),
        internet_service VARCHAR(10),
        internet_type VARCHAR(20),
        avg_monthly_gb_download INT,
        online_security VARCHAR(10),
        online_backup VARCHAR(10),
        device_protection_plan VARCHAR(10),
        premium_tech_support VARCHAR(10),
        streaming_tv VARCHAR(10),
        streaming_movies VARCHAR(10),
        streaming_music VARCHAR(10),
        unlimited_data VARCHAR(10),
        contract VARCHAR(20),
        paperless_billing VARCHAR(5),
        payment_method VARCHAR(30),
        monthly_charge DECIMAL(10,2),
        total_charges DECIMAL(12,2),
        total_refunds DECIMAL(10,2),
        total_extra_data_charges DECIMAL(10,2),
        total_long_distance_charges DECIMAL(12,2),
        total_revenue DECIMAL(12,2),
        customer_status VARCHAR(20),
        churn_category VARCHAR(50),
        churn_reason VARCHAR(255)
    );
    """
    res = run_query(create_telecom_customer_churn_table)
    logging.info("Table creation completed...")
    logging.info("Inserting values to telecom_customer_churn table...")
    insert_into_telecom_customer_churn = '''
        COPY telecom_customer_churn
        FROM 's3://customer-churn-analytics/raw/telecom_customer_churn.csv'
        IAM_ROLE 'arn:aws:iam::950771917939:role/Redshift-S3-access'
        FORMAT AS CSV
        DELIMITER ','
        IGNOREHEADER 1
        EMPTYASNULL
        BLANKSASNULL;
    '''
    run_query(insert_into_telecom_customer_churn)
    logging.info("Value insertion completed...")

# -------------------------------------------------------------------------
# Creating and inserting values to customer_churn_analytics
# -------------------------------------------------------------------------
    logging.info("Creating customer_churn_analytics table...")
    run_query("DROP TABLE IF EXISTS customer_churn_analytics;")
    create_customer_churn_analytics_table = '''
    CREATE TABLE customer_churn_analytics AS
    SELECT
        customer_id,
        gender,
        age,
        city,
        zip_code,
        tenure_in_months,
        monthly_charge,
        total_charges,
        customer_status
    FROM telecom_customer_churn;
    '''
    res = run_query(create_customer_churn_analytics_table)
    logging.info("Table creation completed...")

    # Delete staging table
    run_query('DROP TABLE telecom_customer_churn;')
    logging.info("Deleted staging table...")

# -------------------------------------------------------------------------
# Creating and inserting values to zipcode_population
# -------------------------------------------------------------------------
    logging.info("Creating zipcode_population table...")
    create_zipcode_population_table = '''
    CREATE TABLE IF NOT EXISTS zipcode_population (
        zip_code VARCHAR(10),
        population INT
    );
    '''
    res = run_query(create_zipcode_population_table)
    logging.info("Table creation completed...")
    logging.info("Inserting values to zipcode_population table...")
    insert_into_customer_churn_analytics = '''
    COPY zipcode_population
    FROM 's3://customer-churn-analytics/raw/telecom_zipcode_population.csv'
    IAM_ROLE 'arn:aws:iam::950771917939:role/Redshift-S3-access'
    FORMAT AS CSV
    DELIMITER ','
    IGNOREHEADER 1
    EMPTYASNULL
    BLANKSASNULL;
    '''
    run_query(insert_into_customer_churn_analytics)
    logging.info("Value insertion completed...")

# -------------------------------------------------------------------------
# Creating analytical table churn_analytics
# -------------------------------------------------------------------------
    logging.info("Creating churn_analytics table...")
    run_query("DROP TABLE IF EXISTS churn_analytics;")
    create_churn_analytics_table = '''
    CREATE TABLE churn_analytics
    DISTKEY(zip_code)
    SORTKEY(city)
    AS
    SELECT
        c.customer_id,
        c.city,
        c.zip_code,
        z.population,
        c.tenure_in_months,
        c.monthly_charge,
        c.total_charges,
        c.customer_status
    FROM customer_churn_analytics c
    LEFT JOIN zipcode_population z
    ON c.zip_code = z.zip_code;
    '''
    logging.info("Table creation complete...")

    logging.info("Program execution completed...")
except Exception as e:
    logging.error(f"Error executing query: {e}")