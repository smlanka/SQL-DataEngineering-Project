<h1>Data Dictionary for Gold Layer</h1>
<h2>Overview</h2>
The gld Layer is the business level data representation, structured to support analytical and reporting use cases. It consists of <b>dimension tables</b> and <b>fact tables</b> for specific business metrics
--------------------------------------------------------------------------------------------------------------------------
1. <h3>gold.dim_customer</h3>
<b>Purpose:</b> Store the customer details (along with demographic and geographic data)
<b>Columns and their descriptions</b>:
<img width="738" height="440" alt="image" src="https://github.com/user-attachments/assets/a9ebaa71-82ef-44cd-918b-98d1592bfcf2" />

2.<h3>gold.dim_products</h3>
<b>Purpose:</b> Store the product details (along with their attributes)
<b>Columns and their descriptions</b>:
<img width="587" height="146" alt="image" src="https://github.com/user-attachments/assets/13447273-c6bf-44fc-85aa-21ffe3cea716" />
<img width="589" height="134" alt="image" src="https://github.com/user-attachments/assets/9230ae25-3ead-44cf-9a9d-f6c30b78f0c3" />

2.<h3>gold.fact_sales</h3>
<b>Purpose:</b> Store the transactional sales data for analysis
<b>Columns and their descriptions</b>:
<img width="527" height="221" alt="image" src="https://github.com/user-attachments/assets/30420e47-e679-477d-a819-63c0fc162023" />

