create database olist;

Use olist;

-- 1) Exploration of Dataset

-- Time period of Data
select * from order_reviews;
select min(date(order_purchase_timestamp)),max(date(order_purchase_timestamp)) from orders;

-- Cities and States from where the orders are placed

select customer_state as State,customer_city as City from customers;

-- distribution of total orders as per their status

select order_status,count(*) as order_count from orders
group by order_status
order by order_count desc;

-- Order value of each order

select order_id,price,freight_value,price + freight_value as total_price  from order_items;

-- Growth Trend 

select
time_period,
order_count,
round((((order_count - lag(order_count) over(order by t1.year,t1.month))/ lag(order_count) over(order by t1.year,t1.month))*100),2) 
as growth_percent from (
select extract(year from order_purchase_timestamp) as `year`,
extract(month from order_purchase_timestamp) as `month`,
date_format(order_purchase_timestamp,'%M %Y') as time_period,
count(order_id) as order_count
from orders
where order_status='delivered'
group by `month`,`year`,time_period) t1
order by `year`,`month`;

-- what time do customer tend to buy

select 
temp.purchase_time,
count(*) as total_orders
from (
select order_id,
case 
when time(order_purchase_timestamp) between "00:00:00" and "07:00:00" then "DAWN"
when time(order_purchase_timestamp) between "07:00:01" and "12:00:00" then "Morning"
when time(order_purchase_timestamp) between "12:00:01" and "18:00:00" then "Afternoon"
when time(order_purchase_timestamp) between "18:00:01" and "23:59:59" then "Night"
end
as purchase_time
from orders
) temp
group by purchase_time
order by total_orders desc;

-- Brazilian customers tend to buy in Afternoon and night ---------------------------------

-- Evolution of E-commerce orders in Brazil

-- month on month orders by state

WITH monthly_orders AS (
    SELECT
        c.customer_state AS state,
        YEAR(o.order_purchase_timestamp) AS year,
        MONTH(o.order_purchase_timestamp) AS month,
        DATE_FORMAT(o.order_purchase_timestamp, '%M %Y') AS time_period,
        COUNT(*) AS total_orders
    FROM orders o
    JOIN customers c USING (customer_id)
    GROUP BY state, year, month,time_period
),
lagged_orders AS (
    SELECT
        *,
        LAG(total_orders) OVER (
            PARTITION BY state
            ORDER BY year, month
        ) AS prev_month_orders
    FROM monthly_orders
)
SELECT
    state,
    time_period,
    total_orders,
    prev_month_orders,
    ROUND(
        ((total_orders - prev_month_orders) / prev_month_orders) * 100,
        2
    ) AS MoM_percentage_growth
FROM lagged_orders;


-- Distribution of Customers across state in Brazil

select customer_state as state,count(*) as total_customers
from customers
group by state
order by total_customers desc;

-- % increase in cost of orders from 2017 to 2018

WITH base_data AS (
    SELECT
        o.order_id,
        YEAR(o.order_purchase_timestamp) AS year,
        p.payment_value
    FROM orders o
    JOIN order_payments p USING (order_id) 
    where o.order_status="delivered"
),

monthly_revenue AS (
    SELECT
        year,
        ROUND(SUM(payment_value)) AS total_orders_value
    FROM base_data
    GROUP BY year
)

SELECT
    year,
    total_orders_value,
    ROUND(
        (
            total_orders_value -
            LAG(total_orders_value) OVER (
                ORDER BY year
            )
        ) /
        LAG(total_orders_value) OVER (
            ORDER BY year
        ) * 100,
        2
    ) AS percent_increase
FROM monthly_revenue
ORDER BY year;

-- Jan 2017 vs jan 2018 -----------------------------------------------

WITH base_data AS (
    SELECT
        o.order_id,
        YEAR(o.order_purchase_timestamp) AS year,
        MONTH(o.order_purchase_timestamp) AS month,
        DATE_FORMAT(o.order_purchase_timestamp, '%M %Y') AS Month_n_Year,
        p.payment_value
    FROM orders o
    JOIN order_payments p USING (order_id)
),

monthly_revenue AS (
    SELECT
        year,
        month,
        Month_n_Year,
        ROUND(SUM(payment_value)) AS total_orders_value
    FROM base_data
    GROUP BY year, month, Month_n_Year
)

SELECT
    Month_n_Year,
    total_orders_value,
    ROUND(
        (
            total_orders_value -
            LAG(total_orders_value) OVER (
                PARTITION BY month
                ORDER BY year
            )
        ) /
        LAG(total_orders_value) OVER (
            PARTITION BY month
            ORDER BY year
        ) * 100,
        2
    ) AS percent_increase
FROM monthly_revenue
ORDER BY month, year;


-- Mean and sum of price & freight  value by customer state

select c.customer_state as state ,
round(sum(oi.price)) total_price,
round(avg(oi.price)) avg_price,
round(sum(oi.freight_value)) total_freight,
round(avg(oi.freight_value)) avg_freight
 from order_items oi 
 join orders o 
 using (order_id)
 join customers as c
 on c.customer_id=o.customer_id
 group by state;
 
 
 -- Analysis on sale,freight and delivery time
 
 -- days between purchasing,delivering and actual delivery

select order_id, datediff(order_delivered_customer_date,order_purchase_timestamp)  as actual_delivery_time_in_days ,
datediff(order_estimated_delivery_date,order_purchase_timestamp) as estimated_delivery_time_in_days
from orders
where order_status="delivered" ;

-- groping data by sate,mean of freight_value,time_to_delivery,diff_estimated_delivery

select c.customer_state,round(avg(oi.freight_value),2) as avg_freight_value,
round(avg(datediff(o.order_delivered_customer_date,o.order_purchase_timestamp)),2) as avg_time_to_delivery ,
round(avg(datediff(o.order_estimated_delivery_date,o.order_delivered_customer_date)),2) as avg_diff_estimated_to_delivery
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state;

-- Top 5 states with highest average freight value

select c.customer_state,round(avg(oi.freight_value),2) as avg_freight_value
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state
order by avg_freight_value desc
limit 5 ;

-- States with lowest freight values

select c.customer_state,round(avg(oi.freight_value),2) as avg_freight_value
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state
order by avg_freight_value asc
limit 5 ;

-- Top 5 states with highest average time to delivery

select c.customer_state,
round(avg(datediff(o.order_delivered_customer_date,o.order_purchase_timestamp)),2) as avg_time_to_delivery 
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state
order by avg_time_to_delivery desc
limit 5;

-- States with Lowest average time to delivery

select c.customer_state,
round(avg(datediff(o.order_delivered_customer_date,o.order_purchase_timestamp)),2) as avg_time_to_delivery 
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state
order by avg_time_to_delivery 
limit 5;

-- top 5 states with fast delivery compared to estimated delivery

select c.customer_state,
round(avg(datediff(o.order_estimated_delivery_date,o.order_delivered_customer_date)),2) as avg_diff_estimated_to_delivery 
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state
order by avg_diff_estimated_to_delivery desc
limit 5;

-- States with not so fast delivery compared to estimated date

select c.customer_state,
round(avg(datediff(o.order_estimated_delivery_date,o.order_delivered_customer_date)),2) as avg_diff_estimated_to_delivery 
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state
order by avg_diff_estimated_to_delivery 
limit 5;


-- Payment Type Analysis

-- Month over Month count of orders for different payment types

select time_period,payment_type,count(*) as total_orders
from (
select month(o.order_purchase_timestamp) as month,
year(o.order_purchase_timestamp) as year,
date_format(o.order_purchase_timestamp,"%M %Y") as time_period,
op.payment_type 
from orders o
join order_payments op
using(order_id)) t1
group by t1.month,t1.year,time_period,payment_type
order by t1.year,t1.month;

-- Count of orders based on the no.of payments installments

select payment_installments,count(*) as total_orders 
from order_payments 
group by payment_installments
order by payment_installments;


-- Actionable Insights

-- Total 625 orders were cancelled and 609 are unavailable during the given time_period, which makes it about 1.2% of total orders
-- we can reduce this number by studying the reason behind the cancellation and the items unavailability 

SELECT 
    order_status,
    order_count,
    round(order_count * 100.0 / SUM(order_count) OVER (),2) AS percentage_of_total_orders
FROM (
    SELECT 
        order_status,
        COUNT(*) AS order_count
    FROM orders
    GROUP BY order_status
) t
ORDER BY order_count DESC;

  
-- we can see how orders trajectory is showing very abrupt increase in orders volume with in very short time.
-- Looking at Trend we can see bussiness is picking up very fast in Brazil so comapny has to be ready with extra work force.To avoi high risk,
-- it can hire contractual employess

select
time_period,
order_count,
round((((order_count - lag(order_count) over(order by t1.year,t1.month))/ lag(order_count) over(order by t1.year,t1.month))*100),2) 
as growth_percent from (
select extract(year from order_purchase_timestamp) as `year`,
extract(month from order_purchase_timestamp) as `month`,
date_format(order_purchase_timestamp,'%M %Y') as time_period,
count(order_id) as order_count
from orders
where order_status='delivered'
group by `month`,`year`,time_period) t1
order by `year`,`month`;

-- Company Recieved low rating for maximum orders in few sates;need to study further about the reason for customer disatisfaction to such great
-- extent in these states

SELECT
    c.customer_state,
    SUM(orv.review_score = 1) AS `1`,
    SUM(orv.review_score = 2) AS `2`,
    SUM(orv.review_score = 3) AS `3`,
    SUM(orv.review_score = 4) AS `4`,
    SUM(orv.review_score = 5) AS `5`
FROM order_reviews orv
JOIN orders o USING (order_id)
JOIN customers c USING (customer_id)
GROUP BY c.customer_state;



-- Recommendations 

-- As Brazilian customers usually tend to buy in Afternoon and Night, we can increase the staff during this time frame in order to manage
-- the customers request and services better during this time by reducing workforce of morning and dawn

select 
temp.purchase_time,
count(*) as total_orders
from (
select order_id,
case 
when time(order_purchase_timestamp) between "00:00:00" and "07:00:00" then "DAWN"
when time(order_purchase_timestamp) between "07:00:01" and "12:00:00" then "Morning"
when time(order_purchase_timestamp) between "12:00:01" and "18:00:00" then "Afternoon"
when time(order_purchase_timestamp) between "18:00:01" and "23:59:59" then "Night"
end
as purchase_time
from orders
) temp
group by purchase_time
order by total_orders desc;

-- we can only see 3 states contribute for maximum volume , and rest of the states need to be focused for improving the business

select c.customer_state,count(o.order_id) as total_orders
from customers c
join orders o
on o.customer_id=c.customer_id
group by c.customer_state
order by total_orders desc;

-- Average delivery time is quite hight for most of those states where company is receiving quite less volume of orders,
-- detail study is needed for further checking the other reasons behind such low volume of orders from majority of states.
-- Huge delivery time can be one of the reason and need to work on it

select c.customer_state,
round(avg(datediff(o.order_delivered_customer_date,o.order_purchase_timestamp)),2) as avg_time_to_delivery 
from orders o
join order_items oi
using (order_id) 
join customers c 
on c.customer_id = o.customer_id 
where o.order_status="delivered"
group by c.customer_state
order by avg_time_to_delivery desc;
