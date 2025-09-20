/*
Use this script to perform data checks beforem applying the transformations. Also use the same after pushing the transformed into the silver layer.
In order to do this, the table name must be changed from bronze to silver.
*/

--check for nulls and duplicates in primary key
--expectation new result

select
cst_id,count(*)
from bronze.crm_cust_info
group by cst_id
having count(*)>1 or cst_id is null


--check for unwanted spaces
--How: if the original value is not equal to the value after trimming==> there are spaces
--Expectation: No result
select cst_firstname
from bronze.crm_cust_info
where cst_firstname!=TRIM(cst_firstname)

--check: checking the marital and gender columns
select distinct cst_gndr from bronze.crm_cust_info
select distinct cst_marital_status from bronze.crm_cust_info

select * from silver.crm_cust_info

---------------------For the cust_prod_info-------------------

select prd_id, prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt from bronze.crm_prod_info
--check for nulls and duplicate values
--Expectation no result
select
prd_id,count(*)
from bronze.crm_prod_info
group by prd_id
having count(*)>1 or prd_id is null

--check prd_nm for unmatching after the trim
--Expectation: No result. ==> no trim required
select prd_nm
from bronze.crm_prod_info
where prd_nm!=TRIM(prd_nm)  

--check prd_cost for null/-ve numbers
--Expectation no results
select prd_cost from bronze.crm_prod_info
where prd_cost<0 or prd_cost is null       --There are rows with the unintended results

--Working with prd_line
select distinct prd_line from bronze.crm_prod_info   --Only 4 unique. and one is null

--checking the date columns
--the end date should not be earlier than start date
select * from bronze.crm_prod_info
where prd_end_dt<prd_start_dt
--Here when we do this, there are many prd_ids with the above consition.
--Solution to resolve them:
--1) exchange the start and end date columns. By this we get an overlap of the dates.That is a prd_id will be repeated with different cost in the ssame timeline. 
--2) Ignore the original end_date column, and create our own with the help of start date. end date would be start date-1. THis is fine
--DO final checks for the silver.crm.prod_info. 
select * from silver.crm_prod_info

--------------------------------------For the crm_sales_details--------------------------------------------
--checking if there are unwanted spaces in the sls_ord_num
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_ord_num != trim(sls_ord_num)

--the prd_key from sales_details should be connected to the prod_info table from the silver layer. Checking if there are any extra prd_key in the sales_details
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prod_info)
--Result: No rows

--the cust_id from sales_details should be connected to the cust_info table from the silver layer. Checking if there are any extra cust_id in the sales_details
select 
sls_ord_num,
sls_prd_key,
sls_cust_id,
sls_order_dt,
sls_ship_dt,
sls_due_dt,
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)
--Result: No rows

--checking the date columns
--Dates in integer format
--It must 8 characters (ex: 20500101 -8 digits, year, month, day)
select 
nullif(sls_order_dt,0) sls_ord_dt         --changing to NULL if its 0
from bronze.crm_sales_details
where sls_order_dt<=0               --No negative values but 0's present.
or len(sls_order_dt)!=8
or  sls_order_dt>20500101              --some meaning less values in the date column with only 4 to 5 digits-bad data quality
or sls_order_dt<19000101            --these are boundaries set by the stakeholders

--Do the same checks for ship_dt and due_dt columns as well

--Check if the order_dte is before the ship_dt and due_date
select * from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt>sls_due_dt

--Check the sale, quantity and price
--Sales=Quantity* price, no -ves /0's
select sls_sales, 
sls_quantity, 
sls_price
from bronze.crm_sales_details
where sls_sales!=sls_quantity*sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price
--Issue with sales and price not quantity
--if sales doesnt make sense, multiple price and quantity
--if price -ve, convert it into +ve
--if price 0/null, divide sales/quantity
--The following is the code for it
select sls_sales as sls_sales_old, 
sls_quantity, 
sls_price as sls_price_old,
case when sls_sales is null or sls_sales <=0 or sls_sales!=sls_quantity*abs(sls_price)
	then sls_quantity*abs(sls_price)
	else sls_sales
end as sls_sales,
case when sls_price is null or sls_price<=0
	then sls_sales/nullif(sls_quantity,0)
	else sls_price
end as sls_price
from bronze.crm_sales_details
where sls_sales!=sls_quantity*sls_price
or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <=0 or sls_quantity <=0 or sls_price <=0
order by sls_sales,sls_quantity,sls_price

-----------------------------------------For erp.cust_cust_az12----------------------------------
select 
cid,
bdate,
gen
from bronze.erp_cust_az12

--here the cid is connected the silver.cust_info's cst_key
select 
cid,
bdate,
gen
from bronze.erp_cust_az12
where cid like '%AW00011020%'          --this value from silver.crm_cust_info's cst_key column.
-- The cid column has extraa 3 characters in the beginning
--Resolving it
select 
cid,
case when cid like'NAS%' then SUBSTRING(cid,4,len(cid))
	else cid
end as cid,
bdate,
gen
from bronze.erp_cust_az12
--where                                                      --This is to check if there are any other values in the cid column of the erp's cust_az12 table
--case when cid like'NAS%' then SUBSTRING(cid,4,len(cid))
--	else cid
--end not in (select distinct cst_key from silver.crm_cust_info)

--checking for birth dates (very old dates and if the birth date > current date)
select bdate from bronze.erp_cust_az12
where bdate< '1924-01-01' or bdate> getdate()    --This date is the threshold

--check for the gender column
select distinct gen from bronze.erp_cust_az12    -- many meaningless values. Like F, M, empty string, Male, Female. We need only Male, Female and n/a

select * from silver.erp_cust_az12               -- check after inserting

------------------------------------------------------For erp_loc_a101-------------------------------------------------
select * from bronze.erp_loc_a101
--Here, the cid from bronze.erp_loc_a101 connected to the  cst_key of the silver.crm_cust_info
--Here, the cid from the bronze.erp_loc_a101 has a '-' in it. Removing it now
select cid,
replace (cid,'-','') cid_new,
cntry 
from bronze.erp_loc_a101
where replace (cid,'-','') not in (select cst_key from silver.crm_cust_info)      --checking if there are new values in the cid column of the bronze.erp_loc_a101. There are none

--Checking the country column
select distinct cntry from bronze.erp_loc_a101
order by cntry

select cid,
replace (cid,'-','') cid_new,
case when trim(cntry)='DE' then 'Germany'
	when trim(cntry) in ('US','USA') then 'United States' 
	when trim(cntry)='' or trim(cntry) is null then 'n/a'
	else trim(cntry)
end as cntry 
from bronze.erp_loc_a101

select * from silver.erp_loc_a101                                            --checking after the transformations

------------------------------------------------------For erp_px_cat_g1v2---------------------------------------------
select id, cat,subcat,maintainance from bronze.erp_px_cat_g1v2
--Here the id from the erp_px_cat_g1v2 should be connected with the cat_id from the silver.crm_prod_info table
--Both the columns match==> no manipulation needed

--For the category, subcat and the maintainance columns
select * from bronze.erp_px_cat_g1v2
where cat!=trim(cat) or  subcat!=trim(subcat)  or   maintainance!=trim(maintainance)                   --Making sure no extra spaces

--Checking for distinct values
select distinct cat from bronze.erp_px_cat_g1v2
select distinct subcat from bronze.erp_px_cat_g1v2                      --No nulls or irrelavant values
select distinct maintainance from bronze.erp_px_cat_g1v2

--Checking after pushing into the silver.erp_px_cat_g1v2
select * from silver.erp_px_cat_g1v2
