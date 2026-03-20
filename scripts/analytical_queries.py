# Using the final analytical table, generate the following outputs:
# 1. Churn rate across all customers
# 2. Top cities with the highest number of churned customers
# 3. Customer churn distribution by tenure group
# 4. Total revenue lost due to churn
# 5. Population vs customer count by zip code
# Store the SQL scripts used for generating these results.
from scripts.query_executor import run_query, print_result
import logging

logging.basicConfig(level=logging.INFO)
# -------------------------------------------------------------------------
# 1. Churn rate across all customers
# -------------------------------------------------------------------------
logging.info("Querying churn rate across all customers..")
churn_rate = '''
SELECT 
    ROUND(COUNT(CASE WHEN customer_status = 'Churned' THEN 1 END) * 100.0 / COUNT(*),2) 
    AS churn_rate_percentage
FROM churn_analytics;
'''
result = run_query(churn_rate)
logging.info("Query complete. Displaying results.")
print_result(result)

# -------------------------------------------------------------------------
# 2. Top cities with the highest number of churned customers
# -------------------------------------------------------------------------
logging.info("Querying Top cities with the highest number of churned customers..")
top_cities = '''
SELECT city, COUNT(*) AS churned_customers
FROM churn_analytics
WHERE customer_status ='Churned'
GROUP BY city
ORDER BY churned_customers DESC
'''
result = run_query(top_cities)
logging.info("Query complete. Displaying results.")
print_result(result)

# -------------------------------------------------------------------------
# 3. Customer churn distribution by tenure group
# -------------------------------------------------------------------------
logging.info("Querying Customer churn distribution by tenure group..")
churn_distribution = '''
SELECT
    CASE
        WHEN tenure_in_months BETWEEN 0 AND 12 THEN '0-12 Months'
        WHEN tenure_in_months BETWEEN 13 AND 24 THEN '13-24 Months'
        WHEN tenure_in_months BETWEEN 25 AND 36 THEN '25-36 Months'
        WHEN tenure_in_months BETWEEN 37 AND 48 THEN '37-48 Months'
        ELSE '48+ Months'
    END AS tenure_group,
    COUNT(*) AS churned_customers
FROM churn_analytics
WHERE customer_status = 'Churned'
GROUP BY tenure_group
ORDER BY tenure_group;
'''
result = run_query(churn_distribution)
logging.info("Query complete. Displaying results.")
print_result(result)

# -------------------------------------------------------------------------
# 4. Total revenue lost due to churn
# -------------------------------------------------------------------------
logging.info("Querying Total revenue lost due to churn..")
total_revenue = '''
SELECT SUM(total_charges) AS revenue_lost
FROM churn_analytics
WHERE customer_status = 'Churned';
'''
result = run_query(total_revenue)
logging.info("Query complete. Displaying results.")
print_result(result)

# -------------------------------------------------------------------------
# 5. Population vs customer count by zip code
# -------------------------------------------------------------------------
logging.info("Querying Population vs customer count by zip code..")
population_customer_count = '''
SELECT 
    zip_code, 
    MAX(population) AS total_population,
    COUNT(*) AS customer_count
FROM churn_analytics
GROUP BY zip_code
ORDER BY customer_count DESC;
'''
result = run_query(population_customer_count)
logging.info("Query complete. Displaying results.")
print_result(result)