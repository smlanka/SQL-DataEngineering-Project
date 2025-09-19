/*
==================================================================================================
Create Database and Schemas
==================================================================================================
Purpose:
This is to create a database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database: 'Bronze','Silver','Gold'

Warning:
Running this script will drop the 'DataWarehouse' database if it exists
All the data in the database will be permanantly deleted.
*/


use master;
go

--Drop if exists
if exists (select 1 from sys.databases where name='DataWarehouse')
Begin
  Alter DATABASE Datawarehouse set SINGLE_USER WITH ROLLBACK IMMEDIATE;
  DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database 
create database DataWarehouse;
go

-- use the created database
use DataWarehouse;
go

--Create Schemas
create schema bronze;
go
create schema silver;
go
create schema gold;
go
