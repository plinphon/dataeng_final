
# Football Data Engineering Project
A complete ETL pipeline for football statistics using Apache Airflow, PostgreSQL, and Python.
Overview

This project fetches football player statistics from the StatsBanger API, processes the data through a medallion architecture (Bronze-Silver-Gold), and provides analytical visualizations for performance insights.

Architecture
StatsBanger API → Airflow DAG → PostgreSQL → Matplotlib Visualizations

Data Source: StatsBanger REST API (La Liga season statistics)
Orchestration: Apache Airflow with daily scheduling
Storage: PostgreSQL with Bronze-Silver-Gold layers
Visualization: 7 matplotlib-based analytical functions
