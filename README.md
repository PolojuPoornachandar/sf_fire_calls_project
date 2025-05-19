# SF Fire Calls Analysis Project

## Overview

This project analyzes San Francisco Fire Department call data using **PySpark** on **Databricks**. The goal is to answer key questions about fire call types, response times, and incident locations using a large CSV dataset.

The analysis includes:

- Distinct call types and their counts
- Calls with delayed response times over 5 minutes
- Most common call types and zip codes
- Neighborhoods corresponding to specific zip codes
- Statistical summaries of response times
- Temporal insights by year and week
- Neighborhoods with the worst response times in 2018

## Dataset

The dataset used is the `SF_Fire_Dept_Calls.csv` containing detailed fire call records for San Francisco. It includes columns like:

- Call Number
- Call Type
- Call Date
- Zipcode of Incident
- Neighborhood
- Response Delay (minutes)
- And more...

**Note:** The CSV file should be uploaded to Databricks FileStore or accessible location for the Spark job.

## Environment & Tools

- **Platform:** [Databricks](https://databricks.com/) — cloud-based Apache Spark platform  
- **Language:** Python (PySpark)  
- **Runtime:** Apache Spark with built-in SparkSession  
- **IDE:** Databricks notebook interface or any text editor for `.py` script  
- **No local Apache Spark installation is required**

Databricks provides a fully managed Spark cluster environment which makes running big data workloads simpler and scalable without the overhead of setup and maintenance.

## Project Structure

```plaintext
sf_fire_calls_project/
│
├── sf_fire_calls_analysis.py    # PySpark script with all analysis logic
├── SF_Fire_Dept_Calls.csv       # Dataset (upload separately to Databricks)
├── README.md                    # Project documentation (this file)
├── requirements.txt             # (Optional) Python dependencies (empty or minimal)

