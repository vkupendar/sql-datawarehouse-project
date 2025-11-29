
/*

--------------------------------------------------------------------------
Crate database and Schemas
--------------------------------------------------------------------------

Script Purpose:

This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated.Auditioanally, the script sets up three schemas with in the database.'bronze', 'silver', and 'gold'.

Warning:

Running this script will drop the entire 'DataWarehouse' database it it exists.
All data in the database will be permanently deleted.Proceed with caution and ensure you have proper backups before running the script.
*/

USE master;
GO

--Drop and recreate the 'DataWarehouse' database
IF EXISTS (SELECT 1 FROM SYS.databases where name='DataWarehouse')
BEGIN
  ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
DROP DATABASE DataWarehouse;
END;
GO

--Create the 'DataWarehouse' database

Create DATABASE DataWarehouse;
go


use DataWarehouse;

create schema bronze;--it's like we maintain folders
go--it's like seaparator
create schema silver;
go
create schema gold;
go


