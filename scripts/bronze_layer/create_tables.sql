use DataWarehouse;


--Creating CRM Tables

create table bronze.crm_cust_info (
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(5),
cst_gndr NVARCHAR(5),
cst_create_date DATE
);

create table bronze.crm_prd_info (
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(5),
prd_start_dt DATE,
prd_end_dt DATE
);

/*drop table bronze.prd_info*/


create table bronze.crm_sales_details (
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT
);


--Creating ERP Tables

create table bronze.erp_CUST_AZ12 (
CID NVARCHAR(50),
BDATE DATE,
GEN NVARCHAR(50)
);


create table bronze.erp_LOC_A101 (
CID NVARCHAR(50),
CNTRY NVARCHAR(50)
);


create table bronze.erp_PX_CAT_G1V2 (
ID NVARCHAR(50),
CAT NVARCHAR(50),
SUBCAT NVARCHAR(50),
MAINTENANCE NVARCHAR(5)
);



