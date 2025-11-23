/*

======================================
Creating Database and their Schemas
======================================

This Script purpose is to Create database named DataWarehouse and its Schemas named Bronze, Silver, Gold

Warnings: 

You might need to Drop it if you already have database with same name in your SQL Server


*/

-- Switching to Master database to create our new database
USE master

-- Creating DataWarehouse databse
CREATE DATABASE DataWarehouse

-- Switching to DataWarehouse
USE DataWarehouse

-- Creating Schemas(It is like a Logical container to help us organize things). Using GO tell SQL to execute previous line then proceed and execute the next one
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
