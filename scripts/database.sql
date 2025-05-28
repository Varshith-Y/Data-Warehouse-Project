use master;


--Checking if database exists

if exists(select 1 from sys.databases where name ='DataWarehouse')
begin 
alter database DataWarehouse set single_user with rollback immediate;
drop database DataWarehouse;
end;


--Creating DataBase 

create database DataWarehouse;

use DataWarehouse;

--Creating Schemas

create schema bronze;
create schema silver;
create schema gold;


