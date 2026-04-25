/*
================================================
Create Database and Schemas
================================================
Script Purpose :
	This script creats a new database named 'DataWarehouse' after checking if it
	already esists.
	If the database exists, it is dropeed and recreated. Additionally, the script sets 
	up threee schemas within the database: 'bronze' 'silver', and 'gold'.

WARNING:	
	Running this script will drop the entire 'DataWarehouse' database if it exists.
	All data in the database will be permanently deleted. Proceed with caution
	and ensure you have porper backups before running this script.
	*/

Use master;
Go

-- Drop and recreate the 'Datawarehouse' database
If exists (select 1 from sys.databases where name = 'DataWarehouse')
Begin 
	Alter database DataWarehouse set single_user with rollback Immediate;
	Drop Database DataWarehouse;
End
Go

-- Create Database 'DataWarehouse'
Create Database DataWarehouse;
Go

Use dataWarehouse;
Go

-- Create Schemas

Create Schema bronze;
Go

Create Schema silver;
Go

Create Schema gold;
Go
