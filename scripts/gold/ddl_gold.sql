/*
DDL script for creating the views based on the tables created from the silver layer.
This is for the gold layer. Where the tables are joined based on the business analysis
Each view performs transformations and combines the data from tables from silver layer

These views can be used directly for analytics and reporting
*/


-------------------------------------------customer dimension table-----------------------------------------------
--joining the crm_cust_info, erp_cust_az12 tables and the erp_loc_a101 tables
--the crm table is a master table and left join makes sure no customer are lost
/*
select cst_id,count(*) from(
select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid) t
group by cst_id
having count(*)>1
--The above query checks for duplicates if they are introduced post joining
select 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid

--There are 2 gender columns. Checking them
select distinct
ci.cst_gndr,
ca.gen,
case when ci.cst_gndr!='n/a' then ci.cst_gndr          --Here the crm table's gender is primary.
	else coalesce (ca.gen,'n/a')                --If crm's gndr!=n/a then its the same. If 'n/a' then if exists, fill it with the erp's gen else use 'n/a'
end as new_gen
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
order by 1,2

--gender manipulated query
select 
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr!='n/a' then ci.cst_gndr          --Here the crm table's gender is primary.
	else coalesce (ca.gen,'n/a')                --If crm's gndr!=n/a then its the same. If 'n/a' then if exists, fill it with the erp's gen else use 'n/a'
end as gender ,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid

--generating surrogate key for this dim table
select 
row_number()over(order by cst_id) as customer_key,       --Row_number() to create the surrogate key
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr!='n/a' then ci.cst_gndr          --Here the crm table's gender is primary.
	else coalesce (ca.gen,'n/a')                --If crm's gndr!=n/a then its the same. If 'n/a' then if exists, fill it with the erp's gen else use 'n/a'
end as gender ,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid
*/
--creating view for the customer dimension table
create view gold.dim_customer as
select 
row_number()over(order by cst_id) as customer_key,       --Row_number() to create the surrogate key
ci.cst_id as customer_id,
ci.cst_key as customer_number,
ci.cst_firstname as first_name,
ci.cst_lastname as last_name,
la.cntry as country,
ci.cst_marital_status as marital_status,
case when ci.cst_gndr!='n/a' then ci.cst_gndr          --Here the crm table's gender is primary.
	else coalesce (ca.gen,'n/a')                --If crm's gndr!=n/a then its the same. If 'n/a' then if exists, fill it with the erp's gen else use 'n/a'
end as gender ,
ca.bdate as birthdate,
ci.cst_create_date as create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on ci.cst_key=ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key=la.cid

-------------------------------------------------product dimension--------------------------------------------
/*
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt
from silver.crm_prod_info pn
where prd_end_dt is null                  --Considering the records with no end date. ==> only current products

--joining with the erp_prod_info
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.subcat,
pc.maintainance
from silver.crm_prod_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null  

--Checking if the prd key is unique or not
select prd_key, count(*) from
(
select 
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.subcat,
pc.maintainance
from silver.crm_prod_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null  ) t
group by prd_key
having count(*)>1						-- All the prd_keys are unique no duplicates

--grouping the columns together and renaming them
select 
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintainance as maintenance,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prod_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null 

--creating a surrogate key for the product dimension table
select 
ROW_NUMBER() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintainance as maintenance,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prod_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null 
*/

--creating the view for the product dimension table
create view gold.dim_products as
select 
ROW_NUMBER() over(order by pn.prd_start_dt,pn.prd_key) as product_key,
pn.prd_id as product_id,
pn.prd_key as product_number,
pn.prd_nm as product_name,
pn.cat_id as category_id,
pc.cat as category,
pc.subcat as subcategory,
pc.maintainance as maintenance,
pn.prd_cost as product_cost,
pn.prd_line as product_line,
pn.prd_start_dt as start_date
from silver.crm_prod_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id=pc.id
where prd_end_dt is null 

--------------------------------------------------For sales details table-----------------------------------------
/*
--This needs to be joined with the customer and the product tables from the gold.views as they are joined by the surrogate keys
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,                               --Dont need the sale_details's prd_key and cst_key
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_quantity as quantity,
sd.sls_sales as sales_amount,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id=cu.customer_id
*/

--creating view for the sales_details fact table
create view gold.fact_sales as 
select 
sd.sls_ord_num as order_number,
pr.product_key,
cu.customer_key,                               --Dont need the sale_details's prd_key and cst_key
sd.sls_order_dt as order_date,
sd.sls_ship_dt as ship_date,
sd.sls_due_dt as due_date,
sd.sls_quantity as quantity,
sd.sls_sales as sales_amount,
sd.sls_price as price
from silver.crm_sales_details sd
left join gold.dim_products pr
on sd.sls_prd_key=pr.product_number
left join gold.dim_customer cu
on sd.sls_cust_id=cu.customer_id
