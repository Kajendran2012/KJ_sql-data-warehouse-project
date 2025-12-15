use datawarehouse


--=======================================
       --Performance over time--
--=======================================
-- Analyze the Sales Performance over time --- 

select * from gold.fact_sales

select
year(order_date) as Year1,
sum(sales_amount) as total_sales,
count(customer_key) as total_customer,
sum(quantity) as total_quanttiy
from gold.fact_sales
where order_date is not NULL
group by year(order_date)
order by year(order_date)


--Calculate the total sales for each month 
--and the ruunning total of sales over time -- Cumulative analysis

select * from gold.fact_sales

select
Year1,
Month1,
total_sales,
sum(total_sales) over(partition by year1 order by Year1,Month1) as running_total_sales,
avg(avg_price) over(partition by year1 order by Year1,month1) as moving_average_total_sales
from
(
select 
Year(order_date) As Year1,
month(order_date) As Month1,
sum(sales_amount) as total_sales,
avg(price) as avg_price
from gold.fact_sales
where month(order_date) is not null 
group by Year(order_date), month(order_date)
)t

--=======================================
       --Performance Analysis--
--=======================================

--Analyze the Yearly perfomance of products.
--by comparing each products sales to both
--its average sales performance and the previous Year's sales.

select * from gold.dim_products;

WITH yearly_product_sales AS (
    SELECT 
        YEAR(s.Order_date) AS year1,
        p.product_name,
        SUM(s.sales_amount) AS total_sales
    FROM gold.fact_sales AS s
    LEFT JOIN gold.dim_products AS p
        ON p.product_key = s.product_key
    WHERE YEAR(s.Order_date) IS NOT NULL
    GROUP BY YEAR(s.Order_date), p.product_name
)
SELECT 
year1,
product_name,
total_sales,
avg(total_sales) over(partition by product_name) as avg_sales,
total_sales - avg(total_sales) over(partition by product_name) as Avg_chang,
case 
when total_sales - avg(total_sales) over(partition by product_name) < 0 then 'Below Avg'
when total_sales - avg(total_sales) over(partition by product_name) > 0 then 'Above Avg'
else 'Avg'
end avg_different,
--Year Over Year Analysis -- 
lag(total_sales) over(partition by product_name order by year1) py_sales,
total_sales - lag(total_sales) over(partition by product_name order by year1) diff_py,
case
when total_sales - lag(total_sales) over(partition by product_name order by year1) < 0 then 'Decrease'
when total_sales - lag(total_sales) over(partition by product_name order by year1) > 0 then 'Increase'
else 'No Change'
end avg__py_different
FROM yearly_product_sales
order by product_name, year1

--=======================================
--- Part to Whole Analysis --- 
--=======================================

--Analyze how an individual part is performing 
--compared to the overall allowing us to understand which category 
--has the greates impact on the business.

---Which categories contributes the most overall sales.---

with product_category as
(
    select 
    p.Category as category,
    sum(s.sales_amount) as total_sales
    from gold.fact_sales  s
    left join gold.dim_products p
    on s.product_key = p.product_key
    group by category
)
select 
category,
total_sales,
sum(total_sales) over() as Overal_sales,
concat(round(cast(total_sales as float) / sum(total_sales) over()*100,2),'%') as Percentages
from product_category
order by total_sales desc 

--=======================================
----------- Data Segmentation --------
--=======================================

-- segment products into cost ranges and count how many products fall into each segment

;with cost_segment as
(
    select
    product_key,
    product_name,
    product_cost,
    case
    when product_cost < 100 then 'Below 100'
    when product_cost between 100 and 500 then '100 - 500'
    when product_cost between 500 and 1000 then '500 - 1000'
    else 'Aboce 1000'
    end as segment
    from gold.dim_products)

select
segment,
count(product_key) as total_product
from cost_segment
group by segment
order by total_product desc


/*
Group customers into three segments based on their spending behavior:
-VIP: Cusotmer with at least 12 month of history and spending more than $5,000
-Regular: Customers with at least 12 month of history but spending $5000 or less
- New: Custoemr with a lifespan less than 12Months.
and find the total number of customers by each group
*/

select * from gold.dim_customers
select * from gold.fact_sales

with Customer_spend as
(
    select 
    c.customer_key as customer_key,
    sum(s.sales_amount) as total_spend,
    min(s.order_date) as First_order,
    max(s.order_date) as Last_order,
    datediff(month,  min(s.order_date),max(s.order_date)) as lifespan
    from gold.fact_sales s
    left join gold.dim_customers c
    on c.customer_key = s.customer_key
    group by c.customer_key
    
)
select
Cust_segment,
count(customer_key) as totals_customer
from
(
select 
customer_key,
case 
when total_spend >= 5000 and lifespan > 12 Then 'VIP'
when total_spend <= 5000 and lifespan > 12 Then 'Regular'
else 'New Customer'
end as Cust_segment
from customer_spend)t
group by Cust_segment
order by totals_customer desc














