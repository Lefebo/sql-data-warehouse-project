/*
Gold Layer (Business-Ready Data) 

The Gold layer contains fully refined, trusted, and business-ready datasets that are optimized for analytics, reporting, and downstream applications.
Data in this layer is aggregated, enriched, conformed, and standardized according to business logic.
Gold tables represent final KPIs, dashboards, metrics, and domain-level data products used directly by analysts, BI tools, and machine learning models.
In our Model, we have two dimension table (Product and Customer) and one fact table (sales).
The fact sales table contain the measurable values such as price, sales amount and quantity. The dimension table used to explain in details about the customer 
in dim_Customer table and describe in details about the product in dim_product table.
*/

Create view gold.fact_sales as 
select
       sls_ord_num As order_number,
       pr.product_key,
       cr.customer_key,
       sls_order_dt As order_date,
       sls_ship_dt as ship_date,
       sls_due_dt as due_date, 
       sls_sales as sales_amount,
       sls_quantity as quantity,
       sls_price as price
from silver.crm_sales_details sd
left join gold.dim_customers cr
on sd.sls_cust_id = cr.customer_id
left join gold.dim_product pr
on sd.sls_prd_key = pr.product_number;

/*
Gold Dim product Table:
This dimension product table do have the different categories used to describe more about our products
such as category, sub-category and etc.
*/

Create view gold.dim_product as
select 
   Row_number() over (order by prd.prd_start_dt, prd.prd_key) as product_key,
    prd.prd_id as product_id,
    prd.prd_nm as product_number,
    prd.cat_id as product_name,
    prd.prd_key as category_id,
    cat.CAT as category,
    cat.SUBCAT as sub_category,
    cat.MAINTENANCE as MAINTENANCE,
    prd.prd_cost as cost,
    prd.prd_line as product_line,
    prd.prd_start_dt as production_start_date

from silver.crm_prd_info as prd
left join 
silver.erp_px_cat_g1v2 as cat
on prd.prd_key = cat.ID 
where prd_end_dt Is NULL -- to remove those product where the production date is alkready done

/*
Gold Dim Customer Table:
This dimension customer table are used to describe the different attributes related to customer such 
customer name, gender, birth date and etc
*/

---check also duplicate for prd+key
Create view gold.dim_product as
select 
   Row_number() over (order by prd.prd_start_dt, prd.prd_key) as product_key,
    prd.prd_id as product_id,
    prd.prd_nm as product_number,
    prd.cat_id as product_name,
    prd.prd_key as category_id,
    cat.CAT as category,
    cat.SUBCAT as sub_category,
    cat.MAINTENANCE as MAINTENANCE,
    prd.prd_cost as cost,
    prd.prd_line as product_line,
    prd.prd_start_dt as production_start_date

from silver.crm_prd_info as prd
left join 
silver.erp_px_cat_g1v2 as cat
on prd.prd_key = cat.ID 
where prd_end_dt Is NULL -- to remove those product where the production date is alkready done

