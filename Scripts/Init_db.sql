/*
Create database and schemas
---------------------------

create three schemas which is bronze, silver and gold

*/
use master;

----create datawarehouse database
create database Datawarehouse;
use Datawarehouse;


---Create schema
create schema bronze;
GO
create schema silver;
GO
create schema gold;
GO
