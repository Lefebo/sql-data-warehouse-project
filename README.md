**SQL Server Data Warehouse Project**
**Project Overview**

This project focuses on the design and development of a modern Data Warehouse using SQL Server. The goal is to consolidate sales and operational data from multiple source systems into a structured, high-performance data warehouse optimized for analytical reporting and business intelligence.

**Objective**

Develop a robust and scalable Data Warehouse that:

Integrates data from multiple source systems (ERP and CRM).

Ensures high data quality and consistency.

Provides a clean, unified schema for analytics and reporting.

**Key Features**
**1. Data Integration**

Consolidates data from ERP and CRM systems provided as CSV files.

Maps source data into standardized staging (Bronze), cleansing (Silver), and curated (Gold) layers.

Resolves data quality issues such as duplicates, missing values, and inconsistent formats.

**2. Schema Design**

Implements a Bronze-Silver-Gold architecture:

Bronze Layer: Raw, unprocessed data directly from source systems.

Silver Layer: Cleaned, transformed, and integrated data.

**3. Documentation & Maintenance**

Provides clear documentation of database structure, schemas, and ETL procedures.

Supports ongoing maintenance, version control, and scalability for future enhancements.

Gold Layer: Curated tables and views ready for analytics and reporting.

Supports both fact and dimension tables for structured analytics.

**Technologies Used**

SQL Server / T-SQL

Bulk Insert / ETL Procedures

Database Schema Management (Bronze, Silver, Gold layers)

Data Validation and Transformation Techniques
