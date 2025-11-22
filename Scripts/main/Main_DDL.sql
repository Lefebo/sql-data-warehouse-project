/*
DDL Explanation — Database & Schema Setup

This script resets and prepares a clean SQL Server environment for the data warehouse. It performs the following steps:

Drops the existing database (Datawarehouse_2) if it already exists
Ensures a fresh start without leftover tables, schemas, or corrupted metadata.

Creates a new database named Datawarehouse_2
Sets essential properties such as MULTI_USER mode and disables AUTO_CLOSE for better performance.

Ensures a clean schema structure
Any existing bronze, silver, or gold schemas are dropped safely before re-creating them.

Creates the three core layers of the Medallion Architecture:

Bronze – raw ingested data

Silver – cleaned and standardized data

Gold – business-ready analytical data

This DDL script guarantees that each run starts from a fully clean, consistent, and well-structured environment for ETL/ELT pipelines.
*/
USE master;
GO

-- Drop the database (be careful: this deletes all data!)
DROP DATABASE IF EXISTS Datawarehouse_2;
GO

-- Create database
CREATE DATABASE Datawarehouse_2;
GO

-- Connect to the new database
USE Datawarehouse_2;
GO

-- Ensure multi-user and auto-close off
ALTER DATABASE Datawarehouse_2 SET MULTI_USER;
ALTER DATABASE Datawarehouse_2 SET AUTO_CLOSE OFF;
GO

USE master;
GO

-- Drop the database (be careful: this deletes all data!)
DROP DATABASE IF EXISTS Datawarehouse_2;
GO

-- Create database
CREATE DATABASE Datawarehouse_2;
GO

-- Connect to the new database
USE Datawarehouse;
GO

-- Drop schemas if they exist first
IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'bronze')
    DROP SCHEMA bronze;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'silver')
    DROP SCHEMA silver;
GO

IF EXISTS (SELECT * FROM sys.schemas WHERE name = 'gold')
    DROP SCHEMA gold;
GO

-- Now create schemas
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO



