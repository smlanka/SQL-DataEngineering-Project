/*
THis script is check the quality of the views from gold layer. Do all the necessary checks to make sure that the data is good for analysis
*/

select distinct gender from gold.dim_customer
select  * from gold.dim_products
select  * from gold.fact_sales

--checking for the foreign key integrity
select *
from gold.fact_sales f
left join gold.dim_customer c
on c.customer_key=f.customer_key
left join gold.dim_products p
on p.product_key=f.product_key
where c.customer_key is null    
