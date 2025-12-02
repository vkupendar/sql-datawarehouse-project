/* 
====================================================================
stored procedure : Load SSilver Layer (Bronze -> Silver)
=====================================================================
Script Pupose:
This stored procedure performs the ETL (Extract Transform Load) process to populate the 'silver' tables from the 'bronze' schema.
-Truncates silver tables.
- Insert transformed and cleaned data from bronze into silver tables.


parameters:
None.
This is stored procedure does not accept any parameters or return any values.

Usage example:
exec silver.load_silver

======================================================================
*/



create or alter procedure silver.load_silver as
begin
	
	Declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime;
	begin try
	set @batch_start_time=GETDATE();
	PRINT '=============================================================';
	PRINT 'Loading silver Layer';
	print '=============================================================';

	print '--------------------------------------------------------------';
	print 'Loading CRM Tables';
	print '--------------------------------------------------------------';

	---loading silver.crm_cust_info
	set @start_time=GETDATE();

	print'>> Truncating Table:silver.crm_cust_info';
	Truncate table silver.crm_cust_info;
	print '>> Inserting data into:silver.crm_cust_info';


	Insert into silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date
	)


	select 
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) AS cst_lastname,
	case 
	when upper(TRIM(cst_marital_status))='S' THEN 'Single'
	WHEN UPPER(TRIM(cst_marital_status))='M' then 'Married'
	ELSE 'n/a'
	END 
	cst_marital_status,--Normalise marital status values to readable format
	case 
	when upper(TRIM(cst_gndr))='F' THEN 'Female'
	WHEN UPPER(TRIM(cst_gndr))='M' then 'Male'
	ELSE 'n/a'
	END cst_gndr,--Normalise gender values to readable format
	cst_create_date
	from (
	Select *, row_number() over (partition by cst_id order by cst_create_Date desc) as flag_last
	from bronze.crm_cust_info where cst_id is not null)t
	where flag_last=1;---remove duplicates by taking the most recent data

	set @end_time=GETDATE();
	print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) as nvarchar)+ 'seconds';


	---loading silver.crm_prd_info
	set @start_time=GETDATE();

	print'>> Truncating Table:silver.crm_prd_info';
	Truncate table silver.crm_prd_info;
	print '>> Inserting data into:silver.crm_prd_info';

	Insert into silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt)

	select 
	prd_id,
	REPLACE(SUBSTRING(prd_key,1,5),'-','_') as cat_id,--extraact category id
	SUBSTRING(prd_key,7,len(prd_key)) as prd_key,--extract product key
	prd_nm,
	isnull(prd_cost,0) as prd_cost,
	case upper(trim(prd_line))
		 when 'M' then 'Mountain'
		 when 'R' then 'Road'
		 when 'S' then 'Other Sales'
		 when 'T' then 'Touring'
		 else 'n/a'
		 END AS prd_line,--map product line codes to descriptive values
	cast (prd_start_dt as date) as prd_start_dt,
	cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt --data enrichment-add new, relevent data to enhance the dataset for analysis
	from bronze.crm_prd_info;
	set @end_time=GETDATE();
	print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) as nvarchar)+ 'seconds';

	---loading silver.crm_sales_details
	set @start_time=GETDATE();
	print'>> Truncating Table:silver.crm_sales_details ';
	Truncate table silver.crm_sales_details ;
	print '>> Inserting data into:silver.crm_sales_details ';

	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt=0 or len(sls_order_dt) !=8 then null
			else cast( cast(sls_order_dt as varchar) as date)
	end as sls_order_dt,
	case when sls_ship_dt=0 or len(sls_ship_dt) !=8 then null
			else cast( cast(sls_ship_dt as varchar) as date)
	end as sls_ship_dt,
	case when sls_due_dt=0 or len(sls_due_dt) !=8 then null
			else cast( cast(sls_due_dt as varchar) as date)
	end as sls_due_dt,
	case when sls_sales is null or sls_sales<=0 or 
	sls_sales != sls_quantity* ABS(sls_price)
	then sls_quantity* ABS(sls_price)
	else sls_sales
	end as sls_sales, ---recalculate sales if original value is missing or incorrect
	sls_quantity,
	case when sls_price is null or sls_price<=0
	then sls_sales/NULLIF(sls_quantity,0)
	ELSE sls_price
	end as sls_price --derive price if originl value is invalid
	from bronze.crm_sales_details;
	set @end_time=GETDATE();
	print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) as nvarchar)+ 'seconds';
	---loading silver.erp_cust_az12
	set @start_time=GETDATE();

	print'>> Truncating Table:silver.erp_cust_az12 ';
	Truncate table silver.erp_cust_az12 ;
	print '>> Inserting data into:silver.erp_cust_az12 ';


	insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)

	select
	case when cid like 'NAS%' THEN SUBSTRING(cid, 4, len(cid))
	else cid
	end cid,---remove 'NAS' prefix if present
	case when bdate >getdate() then null
	else bdate
	end as bdate,--set future birthdates to NULL
	case when upper(trim(gen)) in ('F','FEMALE') THEN 'Female'
	when upper(trim(gen)) in ('M','MALE') THEN 'Male'
	else 'n/a'
	end as gen--NORMALIZE GENDER VALUES AND HANDLE UNKNOWN CASES
	from bronze.erp_cust_az12;

	set @end_time=GETDATE();
	print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) as nvarchar)+ 'seconds';

	---loading silver.ERP_LOC_A101
	set @start_time=GETDATE();

	print'>> Truncating Table:silver.ERP_LOC_A101 ';
	Truncate table silver.ERP_LOC_A101;
	print '>> Inserting data into:silver.ERP_LOC_A101';

	insert into silver.ERP_LOC_A101
	(cid,cntry)

	SELECT 
	Replace(cid,'-','') cid,
	case when trim(cntry)='DE' then 'Germany'
		when trim(cntry) in ('US','USA') THEN 'United States'
		when trim(cntry)='' or cntry is null then 'n/a'
		else trim(cntry)
		end as
	cntry---normalise an handle missing or blank country codes
	FROM BRONZE.ERP_LOC_A101

	set @end_time=GETDATE();
	print '>> Load Duration: '+ CAST(DATEDIFF(SECOND,@start_time, @end_time) as nvarchar)+ 'seconds';

	---loading silver.erp_px_Cat_g1v2
	set @start_time=GETDATE();


	print'>> Truncating Table:silver.erp_px_Cat_g1v2 ';
	Truncate table silver.erp_px_Cat_g1v2 ;
	print '>> Inserting data into:silver.erp_px_Cat_g1v2 ';

	insert into silver.erp_px_Cat_g1v2 ( id,cat,subcat,maintenance)
	select id,
	cat,subcat,
	maintenance from bronze.erp_px_Cat_g1v2

	set @batch_end_time=GETDATE();
	print '=================================================';
	print 'Loading Silver Layer is completed';
	print'  -Total load duration: ' +cast(datediff(second,@batch_start_time,@batch_end_time) as nvarchar) + 'second';
	print '====================================================================='

	END TRY

	BEGIN CATCH

	PRINT '=========================================================================';

	PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
	PRINT 'ERROR MESSAGE' +ERROR_MESSAGE();
	PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
	PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
	PRINT '================================================================================';
	END CATCH
end
