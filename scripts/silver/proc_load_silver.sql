/*
This is the script for the Silver Layer's stored procedure. (T in the ETL process)
Transfering the transformed data into the silver layer tables.
It has the Data transformation, standardization and enrichment.
*/

create or ALTEr procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime,@batch_start_time datetime,@batch_end_time datetime
	begin try
		set @batch_start_time=GetDate();
		print'>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>Loading the Silver Layer<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<<'
		--selecting the most recently created row in case of duplicates in primary key
		/*select
		* from(
		select
		* ,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info) t where flag_last=1

		--Trimming the string columns of unwanted spaces
		select cst_id , cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname, cst_marital_status, cst_gndr, cst_create_date
		from(
		select
		* ,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info) t where flag_last=1


		*/
		--Inserting into the silver.crm_cust_info
		print'(((((((((((((((((((((((((((((((((((((((((Loading the CRM Tables))))))))))))))))))))))))))))))))))))))))'
		set @start_time=getDate();
		truncate table silver.crm_cust_info;
		print '======================Inserting into silver.crm_cust_info table==============='
		insert into silver.crm_cust_info(
		cst_id , cst_key, cst_firstname,cst_lastname, cst_marital_status,cst_gndr, cst_create_date
		)

		--Changing the marital and gender from abbrevations to full forms
		select cst_id , cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname, 
		case when upper(trim(cst_marital_status))='S'  then 'Single'
			when upper(trim(cst_marital_status))='Married' then 'Married'
			else 'n/a'
		end cst_marital_status,
		case when upper(trim(cst_gndr))='F'  then 'Female'
			when upper(trim(cst_gndr))='M' then 'Male'
			else 'n/a'
		end cst_gndr,
		cst_create_date
		from(
		select
		* ,
		ROW_NUMBER() over(partition by cst_id order by cst_create_date desc) as flag_last
		from bronze.crm_cust_info) t where flag_last=1
		set @end_time=GetDate();
		print'--Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds--'
		print'==============================inserting into silver.crm_cust_info done ===================================='
	

		---------------------For the cust_prod_info---------------------------------------
		/*
		--Splitting the prd_key into 2 (at the 5th charater) cause the first 5 characters are category id
		select prd_id, prd_key,SUBSTRING(prd_key,1,5) as cat_id,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from bronze.crm_prod_info

		--select * from bronze.erp_px_cat_g1v2   --In the id column of the erp's category table, the delimiter is _ not -. Replacing it below

		select prd_id, prd_key,Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from bronze.crm_prod_info
		where Replace(SUBSTRING(prd_key,1,5),'-','_') not in(select distinct id from bronze.erp_px_cat_g1v2)  --this is to check and make sure that all the cat_id from prod_info table in the erp's cat table. 
		--CO_PE this cat from the erp's cat table is not available in the prod_info table  (thats fine)

		--splitting the prd_key from the 7th position to the end to join it with the prd_key from the sales_details table.
		select prd_id, substring(prd_key,7,len(prd_key)) as prd_key,Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from bronze.crm_prod_info
		where substring(prd_key,7,len(prd_key)) in (select sls_prd_key from bronze.crm_sales_details)  --When do this there are many prd_key values in the prod info that are not in the sales_details.
																										--So, only considering the actual overlap values. with 'IN' not 'Not in'
		select prd_id,prd_key, substring(prd_key,7,len(prd_key)) as prd_key,Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from bronze.crm_prod_info

		--Working with prd_nm
		--replacing null values with 0
		select prd_id,prd_key, substring(prd_key,7,len(prd_key)) as prd_key,Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		prd_line,prd_start_dt,prd_end_dt from bronze.crm_prod_info

		--Working with prd_line
		select prd_id,prd_key, substring(prd_key,7,len(prd_key)) as prd_key,Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
				when 'M' then 'Mountain'            --Easier to map the replacing values this way
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				Else 'n/a'
		end as prd_line,
		prd_start_dt,prd_end_dt from bronze.crm_prod_info

		--Working with start and end date
		select prd_id,prd_key, substring(prd_key,7,len(prd_key)) as prd_key,Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
				when 'M' then 'Mountain'            --Easier to map the replacing values this way
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				Else 'n/a'
		end as prd_line,
		cast(prd_start_dt AS Date) AS prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		from bronze.crm_prod_info
		*/
		--Inserting into the silver.prod_info column
		set @start_time=Getdate();
		truncate table silver.crm_prod_info;
		print'========================inserting into the silver.crm_prod_info============================== '
		insert into silver.crm_prod_info(
		prd_id,cat_id, prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)

		select 
		prd_id,Replace(SUBSTRING(prd_key,1,5),'-','_') as cat_id, substring(prd_key,7,len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost,0) as prd_cost,
		case upper(trim(prd_line))
				when 'M' then 'Mountain'            --Easier to map the replacing values this way
				when 'R' then 'Road'
				when 'S' then 'Other Sales'
				when 'T' then 'Touring'
				Else 'n/a'
		end as prd_line,
		cast(prd_start_dt AS Date) AS prd_start_dt,
		cast(lead(prd_start_dt) over (partition by prd_key order by prd_start_dt)-1 as date) as prd_end_dt
		from bronze.crm_prod_info
		set @end_time=GetDate();
		print'--Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds--'
		print'================================inserting into the silver.crm_prod_info done=========================='

		--------------------------------------For crm_sales_details----------------------------------------
		/*--Manipulating the date columns
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
			else cast(cast(sls_order_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_order_dt,
		case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then NULL
			else cast(cast(sls_ship_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_ship_dt,
		case when sls_due_dt=0 or len(sls_due_dt)!=8 then NULL
			else cast(cast(sls_due_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		from bronze.crm_sales_details
		--Manipulating the sales, price and quantity columns
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
			else cast(cast(sls_order_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_order_dt,
		case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then NULL
			else cast(cast(sls_ship_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_ship_dt,
		case when sls_due_dt=0 or len(sls_due_dt)!=8 then NULL
			else cast(cast(sls_due_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price)
			then sls_quantity*abs(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price<=0
			then sls_sales/nullif(sls_quantity,0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_details
		*/
		--Inserting into the silver.crm_sales_details columns
		set @start_time=getDate();
		truncate table silver.crm_sales_details;
		print'==================================inserting into silver.crm_sales_details=================================='
		insert into silver.crm_sales_details(
		sls_ord_num ,
		sls_prd_key ,
		sls_cust_id ,
		sls_order_dt,
		sls_ship_dt ,
		sls_due_dt ,
		sls_sales,
		sls_quantity,
		sls_price
		)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case when sls_order_dt=0 or len(sls_order_dt)!=8 then NULL
			else cast(cast(sls_order_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_order_dt,
		case when sls_ship_dt=0 or len(sls_ship_dt)!=8 then NULL
			else cast(cast(sls_ship_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_ship_dt,
		case when sls_due_dt=0 or len(sls_due_dt)!=8 then NULL
			else cast(cast(sls_due_dt as varchar)as Date)             --Because, int to date not possible in sql server
		end as sls_due_dt,
		case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price)
			then sls_quantity*abs(sls_price)
			else sls_sales
		end as sls_sales,
		sls_quantity,
		case when sls_price is null or sls_price<=0
			then sls_sales/nullif(sls_quantity,0)
			else sls_price
		end as sls_price
		from bronze.crm_sales_details
		set @end_time=GetDate();
		print'--Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds--'
		print'===================================inserting into silver.crm_sales_details done================================='
		print'(((((((((((((((((((((((((((((((((((((Loading the CRM tables done))))))))))))))))))))))))))))))))))))))))))))))))'

		----------------------------------------For erp.cust_az12----------------------------------------
		/*--Checking the cid column. It needs to be connected to the cst_key from silver.crm_cust_info
		select 
		case when cid like'NAS%' then SUBSTRING(cid,4,len(cid))                --Removing the extra characters from the beginning for the cid column
			else cid
		end as cid,
		bdate,
		gen
		from bronze.erp_cust_az12

		--Manipulating the bdate and accomdating for dates > current date
		select 
		case when cid like'NAS%' then SUBSTRING(cid,4,len(cid))                --Removing the extra characters from the beginning for the cid column
			else cid
		end as cid,
		case when bdate> getdate() then null
			else bdate
		end as bdate,
		bdate,
		gen
		from bronze.erp_cust_az12

		--Manipulating the gender column
		select 
		case when cid like'NAS%' then SUBSTRING(cid,4,len(cid))                --Removing the extra characters from the beginning for the cid column
			else cid
		end as cid,
		case when bdate> getdate() then null
			else bdate
		end as bdate,
		bdate,
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			when upper(trim(gen)) in ('M','MALE') then 'Male'
			else 'n/a'
		end as gen
		from bronze.erp_cust_az12
		*/

		--Inserting into silver.erp_cust_az12
		print'((((((((((((((((((((((((((((((((((((((Loading the ERP tables))))))))))))))))))))))))))))))))))))))))))))))'
		set @start_time=GETDATE();
		truncate table silver.erp_cust_az12;
		print'===============================inserting into silver.erp_cust_az12========================================'
		insert into silver.erp_cust_az12 (cid, bdate, gen)
		select 
		case when cid like'NAS%' then SUBSTRING(cid,4,len(cid))                --Removing the extra characters from the beginning for the cid column
			else cid
		end as cid,
		case when bdate> getdate() then null
			else bdate
		end as bdate,
		case when upper(trim(gen)) in ('F','FEMALE') then 'Female'
			when upper(trim(gen)) in ('M','MALE') then 'Male'
			else 'n/a'
		end as gen
		from bronze.erp_cust_az12
		set @end_time=GetDate();
		print'--Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds--'
		print'===============================inserting into silver.erp_cust_az12 done========================================'



		------------------------------------------------------For erp_loc_a101-------------------------------------------------
		/*--Here, the cid from bronze.erp_loc_a101 connected to the  cst_key of the silver.crm_cust_info
		--Here, the cid from the bronze.erp_loc_a101 has a '-' in it. Removing it now
		select
		replace (cid,'-','') cid,  
		case when trim(cntry)='DE' then 'Germany'
			when trim(cntry) in ('US','USA') then 'United States' 
			when trim(cntry)='' or trim(cntry) is null then 'n/a'
			else trim(cntry)
		end as cntry 
		from bronze.erp_loc_a101
		*/
		--Inserting into the silver.erp_loc_a101
		set @start_time=Getdate();
		truncate table silver.erp_loc_a101
		print'===============================inserting into silver.erp_loc_a101========================================'
		insert into silver.erp_loc_a101(cid,cntry)
		select
		replace (cid,'-','') cid,  
		case when trim(cntry)='DE' then 'Germany'
			when trim(cntry) in ('US','USA') then 'United States' 
			when trim(cntry)='' or trim(cntry) is null then 'n/a'
			else trim(cntry)
		end as cntry 
		from bronze.erp_loc_a101
		set @end_time=GetDate();
		print'--Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds--'
		print'===============================inserting into silver.erp_loc_a101 done========================================'

		------------------------------------------------------For erp_px_cat_g1v2---------------------------------------------
		/*
		--All the columns are good. No issues So direct push into silver.erp_px_cat_g1v2
		*/
		set @start_time=getdate();
		truncate table silver.erp_px_cat_g1v2;
		print'============================inserting into silver.erp_px_cat_g1v2====================================='
		insert into silver.erp_px_cat_g1v2 (id, cat,subcat,maintainance)
		select id, cat,subcat,maintainance from bronze.erp_px_cat_g1v2
		set @end_time=GetDate();
		print'--Load Duration: '+cast(datediff(second,@start_time,@end_time)as nvarchar)+'seconds--'
		print'==================================inserting into silver.erp_px_cat_g1v2 done==================================='
		print'((((((((((((((((((((((((((((((((((Loading the ERP tables done)))))))))))))))))))))))))))))))))))))))))))))'
		set @batch_end_time=GetDate();
		print'--Total Load Duration: '+cast(datediff(second,@batch_start_time,@batch_end_time)as nvarchar)+'seconds--'
	end try
	begin catch
		print'================================================='
		print 'Error occured during loading the bronze layer'
		print'Error Message'+ERROR_MESSAGE();
		print'Error Message'+CAST(ERROR_NUMBER() as nvarchar);
		print 'Error Message' + cast(error_STATE() as nvarchar);
	end catch
End

--exec silver.load_silver                                      : use this to run this stored proceddure
