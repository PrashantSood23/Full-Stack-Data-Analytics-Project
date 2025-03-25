SET sql_mode = ''; #it is used to restrict the safety modes[be carefull while using this]
#SET FOREIGN_KEY_CHECKS = 0; [used to delete a table which has foreign key]
#DROP TABLE orders;
#SET FOREIGN_KEY_CHECKS = 1; [set the foreign key again]
SET GLOBAL max_allowed_packet = 1073741824;  -- (1GB) #Handles large query results.
SET GLOBAL wait_timeout = 600;  #Increases idle session time before disconnecting.
SET GLOBAL interactive_timeout = 600;  #Increases idle session time before disconnecting.
SET GLOBAL net_read_timeout = 600;  #Extends the time MySQL waits for data.
SET GLOBAL net_write_timeout = 600;   #Extends the time MySQL waits for data.


##########################################################################################################################################################
CREATE DATABASE project;
USE project;

#Creating table df and loading data from csv file
CREATE TABLE df (
    order_id VARCHAR(50),customer_id VARCHAR(50),
    order_status VARCHAR(20),order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,order_estimated_delivery_date DATETIME,
    order_approved_at_flag varchar(20),order_delivered_carrier_date_flag varchar(20),
    order_delivered_customer_date_flag varchar(20),
    customer_unique_id VARCHAR(50),customer_zip_code_prefix INT,
    customer_city VARCHAR(100),customer_state VARCHAR(10),
    payment_sequential INT,payment_type VARCHAR(20),
    payment_installments INT,payment_value float,
    review_id VARCHAR(50),review_score INT,
    review_comment_title text ,review_comment_message TEXT,
    review_creation_date DATETIME,review_answer_timestamp DATETIME,
    order_item_id INT,product_id VARCHAR(50),
    seller_id VARCHAR(50),shipping_limit_date DATETIME,
    price float,shipping_cost DECIMAL(10,2),
    product_category_name VARCHAR(100),product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,product_weight_g float,
    product_length_cm float,product_height_cm float,
    product_width_cm float,delivery_days INT,
    delivery_delay_days INT,payment_per_installment DECIMAL(10,2),
    payment_price_gap DECIMAL(10,2),review_sentiment VARCHAR(100),
    delievery_status VARCHAR(20),
    Recency INT,Frequency INT,Monetary DECIMAL(10,2),
    purcahse_day_of_week VARCHAR(10),purchase_hour INT,
    review_cotains_bad BOOLEAN,review_contains_good BOOLEAN,
    customer_clv_proxy DECIMAL(10,2)
);
load data infile 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\df_cleaned.csv'
into table df
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows;


#Creating Customers Table and Importing data from csv
#[COLUMN NAMES THAT NEEDED] -- customer_id,customer_unique_id,customer_zip_code_prefix,customer_city,customer_state
CREATE TABLE customers(customer_id VARCHAR(50) PRIMARY KEY,
customer_zip_code_prefix VARCHAR(50), customer_city VARCHAR(50),
 customer_state VARCHAR(50) );
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\customers_cleaned.csv' 
INTO TABLE customers
FIELDS TERMINATED BY ',' 
ENCLOSED BY '"' 
LINES TERMINATED BY '\r\n' 
IGNORE 1 ROWS (customer_id,@dummy,customer_zip_code_prefix,customer_city,customer_state);
SELECT * FROM customers WHERE customer_id='00050bf6e01e69d5c0fd612f1bcfb69c';
TRUNCATE TABLE customers;


#Create orders table and importing data from csv
#[COLUMN NAMES THAT NEEDED] order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,
-- order_estimated_delivery_date,order_approved_at_flag,order_delivered_carrier_date_flag,order_delivered_customer_date_flag
CREATE TABLE orders(order_id VARCHAR(50) PRIMARY KEY,
 customer_id VARCHAR(50) NULL, order_status VARCHAR(20),order_purchase_timestamp DATETIME NULL,
 order_approved_at DATETIME NULL,order_delivered_carrier_date DATETIME NULL,
 order_delivered_customer_date DATETIME NULL,order_estimated_delivery_date DATETIME NULL,
 order_approved_at_flag VARCHAR(20),order_delivered_carrier_date_flag VARCHAR(20),
 order_delivered_customer_date_flag VARCHAR(20),
 FOREIGN KEY(customer_id) REFERENCES customers(customer_id));
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\orders_cleaned.csv'
 INTO TABLE orders
 FIELDS TERMINATED BY ',' 
 ENCLOSED BY '"' 
 LINES TERMINATED BY '\n' 
 IGNORE 1 ROWS 
 (order_id,customer_id,order_status,order_purchase_timestamp,order_approved_at,order_delivered_carrier_date,order_delivered_customer_date,
order_estimated_delivery_date,order_approved_at_flag,order_delivered_carrier_date_flag,order_delivered_customer_date_flag);




#Creating order_items table and importing data from csv
#[COLUMN NAMES THAT NEEDED] order_id,order_item_id,product_id,seller_id,shipping_limit_date,price,shipping_cost
CREATE TABLE order_items(order_id VARCHAR(50), order_item_id VARCHAR(50), 
 product_id VARCHAR(50), seller_id VARCHAR(50), shipping_limit_date DATETIME, price FLOAT,
 shipping_cost FLOAT, FOREIGN KEY(order_id) REFERENCES orders(order_id));
 LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\order_items_cleaned.csv'
 INTO TABLE order_items
 FIELDS TERMINATED BY ','
 ENCLOSED BY '"'
 LINES TERMINATED BY '\n'
 IGNORE 1 ROWS;
 
 
 # creating table products, and importing data from csv
 #[COLUMN NAMES THAT NEEDED] product_id,product_category_name,product_name_length,product_description_length,product_photos_qty,
 -- product_weight_g,product_length_cm,product_height_cm,product_width_cm
CREATE TABLE products(product_id VARCHAR(50) PRIMARY KEY, product_category_name VARCHAR(80),
product_weight_g FLOAT, product_length_cm FLOAT, product_height_cm FLOAT, product_width_cm FLOAT);

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\products_cleaned.csv'
INTO TABLE products
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id,product_category_name,@dummy,@dummy,@dummy,
 product_weight_g,product_length_cm,product_height_cm,product_width_cm);



#creating table sellers, and importing data from csv
#[COLUMN NAMES THAT NEEDED] seller_id,seller_zip_code_prefix,seller_city,seller_state
CREATE TABLE sellers(seller_id VARCHAR(50) PRIMARY KEY,seller_zip_code_prefix INT(8), seller_city VARCHAR(20), seller_state VARCHAR(50));
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\sellers_cleaned.csv'
INTO TABLE sellers
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;
DROP TABLE sellers;
ALTER TABLE sellers MODIFY seller_city VARCHAR(50); 


#creating table order_payments and importing data from csv
#order_id,payment_sequential,payment_type,payment_installments,payment_value
CREATE TABLE order_payments(order_id VARCHAR(50),payment_sequential INT, 
payment_type VARCHAR(20), payment_installments INT, 
payment_value FLOAT, FOREIGN KEY(order_id) REFERENCES orders(order_id));

LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\order_payments_cleaned.csv'
INTO TABLE order_payments
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;


#creating tabke order_reviews and importing data from csv
#review_id,order_id,review_score,review_comment_title,review_comment_message,review_creation_date,review_answer_timestamp
CREATE TABLE order_reviews(review_id VARCHAR(50) PRIMARY KEY, 
order_id VARCHAR(50), review_score INT, review_comment_message VARCHAR(300),
 FOREIGN KEY(order_id) REFERENCES orders(order_id));
LOAD DATA INFILE 'C:\\ProgramData\\MySQL\\MySQL Server 8.0\\Uploads\\order_reviews_cleaned.csv'
INTO TABLE order_reviews
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(review_id,order_id,review_score,@dummy,review_comment_message,@dummy,@dummy);
DROP TABLE order_reviews;
ALTER TABLE order_reviews DROP PRIMARY KEY;
ALTER TABLE order_reviews MODIFY review_comment_message VARCHAR(1000);


# Business Queries(Insights)

# 1. Total number of order placed
SELECT count(order_id) as total_orders FROM orders;

# 2.1 Total revenue generated
select sum(payment_value) as total_revenue from order_payments;

# 2.2 Total Revenue generated by orders that are delivered
select sum(op.payment_value) as total_revenue from order_payments op 
join orders o on o.order_id=op.order_id where order_status ='Delivered';

# 2.3 Total revenue generated by orders that are not delivered
select sum(op.payment_value) as total_revenue from order_payments op 
join orders o on o.order_id=op.order_id where order_status !='Delivered'; 


# 3. Top 10 cutomers by total spend
select o.customer_id, sum(op.payment_value) as total_spent 
from order_payments op join orders o on o.order_id=op.order_id 
group by o.customer_id order by total_spent desc limit 10;

# 4. Top selling products by quantity
select product_id, count(*) from order_items 
group by product_id 
having count(*)>1 
order by count(*) desc ;

# 5. Top products by total sales values(Price-shipping)
select product_id, truncate(price+shipping_cost,2) as price_and_shipping 
from order_items order by price_and_shipping desc limit 10;

# 6. How many orders has been delivered and how many not
select  order_status, count(order_status) from orders group by order_status ;


# 7. Monthly revenue trend [in this monthly revenue trend, its also calculating the payments 
-- of those orders which are not delivered, and doesnt have payment recorded in payments_value]
select date_format(order_purchase_timestamp,'%Y-%m') as months, 
sum( payment_value) as monthly_revenue from orders o
join order_payments op on o.order_id=op.order_id group by months order by months;

# 8. Monthly revenue trend of orders that are delivered successfuly
SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS months,
    SUM(op.payment_value) AS monthly_revenue
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
JOIN order_items oi ON o.order_id = oi.order_id  
WHERE o.order_status = 'delivered'  
GROUP BY months
ORDER BY months;

# 9. Number of customers by  state
select  customer_state, count(distinct customer_id) as total_customer from customers
group by  customer_state order by total_customer desc limit 10;
 
# 10. Number of customers by city
select  customer_city, count(distinct customer_id) as total_customer from customers
group by  customer_city order by total_customer desc limit 10;

# 11. Orders with late delivery(delivered after estimated date)
select order_id, customer_id,order_status,order_purchase_timestamp,
order_approved_at,order_delivered_carrier_date,date_format(order_estimated_delivery_date, '%Y-%m-%d') as estimated_date,
date_format(order_delivered_customer_date, '%Y-%m-%d') as delievered_date from orders
where order_estimated_delivery_date < order_delivered_customer_date;


# 12. Average Delievery Delay[only for orders which are late from the estimated delievery date]
select avg(datediff(order_delivered_customer_date,order_estimated_delivery_date)) as avg_delay_days
from orders where order_estimated_delivery_date < order_delivered_customer_date;

# 13. Total numbers of orders per payment method
select payment_type, count(distinct order_id) as total_orders from order_payments
group by payment_type order by total_orders desc;

# 14. Average order value
select avg(payment_value) as average_order_value from order_payments;

# 15. Customer lifetime value by customer
select distinct customer_id, customer_clv_proxy from df order by customer_clv_proxy desc limit 10; 

# 16. churned customers(never ordered again, ordered only once)	
select customer_id from orders group by customer_id having count(order_id)=1;

# 17. sales contribution by products with shipping cost
select product_category_name, sum(payment_value) from df 
where product_category_name='cool_stuff'group by product_category_name ; 

# 18. sales contribution by products without shipping cost
select sum(ot.price), p.product_category_name from order_items ot 
join products p on ot.product_id=p.product_id group by p.product_category_name;

# 19. Most comman number of installment used in payments
select payment_installments, count(*) as count from order_payments
group by payment_installments order by count desc limit 5;

# 20. Customers with high purchase history
select customer_id, count(order_id) as total_orders from orders
group by customer_id order by total_orders desc;

# 21. Revenue by month and product category[Delivered orders only]
SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS revenue_month,
    p.product_category_name,
    SUM(op.payment_value) AS total_revenue
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE o.order_status = 'delivered'  
GROUP BY revenue_month, p.product_category_name
ORDER BY revenue_month, total_revenue DESC;


# 22. Failed delievery orders revenue by month
SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS month, 
    COUNT(DISTINCT o.order_id) AS failed_orders_count, 
    SUM(op.payment_value) AS failed_orders_revenue
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_status NOT IN ('delivered')  
GROUP BY month
ORDER BY month;


# 23. orders that were delivered but have no recorded payment
SELECT o.order_id, o.order_purchase_timestamp, o.customer_id, o.order_status
FROM orders o
LEFT JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_status = 'delivered' 
AND op.order_id IS NULL;

# 24. To find orders that failed but still have a recorded payment
SELECT o.order_id, o.order_purchase_timestamp, o.customer_id, o.order_status, 
       SUM(op.payment_value) AS total_payment
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_status !='Delivered' 
GROUP BY o.order_id, o.order_purchase_timestamp, o.customer_id, o.order_status;

# 25. count To find orders that failed but still have a recorded payment
SELECT COUNT(*) AS failed_orders_with_payment
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
WHERE o.order_status !='Delivered';


# 26. Top performing states/regions in terms of revenue [State wise]
select distinct customer_state, sum(payment_value) as total_revenue from df 
group by customer_state
order by total_revenue desc;

# 27. Top performing states/regions in terms of revenue [city wise]
select distinct customer_city, sum(payment_value) as total_revenue from df 
group by customer_city
order by total_revenue desc;


# 28. Total payment as per product
select oi.order_id, oi.product_id,oi.price,
oi.shipping_cost,oi.price+oi.shipping_cost as total_payment_per_product
from order_items oi join order_payments op on oi.order_id=op.order_id;




# Creating views so that power BI can directly query these views to reduce the complexity inside

# Total revenue generated
create view vw_total_rev as 
select sum(payment_value) as total_revenue from order_payments;

#  Total Revenue generated by orders that are delivered
create view vw_total_rev_delivered as 
select sum(op.payment_value) as total_revenue from order_payments op 
join orders o on o.order_id=op.order_id where order_status ='Delivered';

#  Total revenue generated by orders that are not delivered
create view vw_total_revenue_not_delivered as 
select sum(op.payment_value) as total_revenue from order_payments op 
join orders o on o.order_id=op.order_id where order_status !='Delivered'; 

#Monthly revenue trend of orders that are delivered successfuly
create view vw_monthly_revenue as 
SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS months,
    SUM(op.payment_value) AS monthly_revenue
FROM orders o
JOIN order_payments op ON o.order_id = op.order_id
JOIN order_items oi ON o.order_id = oi.order_id  
WHERE o.order_status = 'delivered'  
GROUP BY months
ORDER BY months;

#top 10 customers
#create view vw_top_customers as 
select o.customer_id, sum(op.payment_value) as total_spent 
from order_payments op join orders o on o.order_id=op.order_id 
group by o.customer_id order by total_spent desc limit 10;


#Least 10 customers
#create view vw_top_customers as 
select o.customer_id, sum(op.payment_value) as total_spent 
from order_payments op join orders o on o.order_id=op.order_id 
group by o.customer_id order by total_spent limit 10;

#Top products by qty
#create view vw_top_products as 
select product_id, count(*) from order_items 
group by product_id 
having count(*)>1 
order by count(*) desc ;


# order with delay deliveey date
create view vw_order_with_delays as 
select order_id, order_estimated_delivery_date, order_delivered_customer_date from orders 
where order_estimated_delivery_date < order_delivered_customer_date order by order_estimated_delivery_date ;

#Total order per payment method
create view vw_payment_method as 
select payment_type, count(distinct order_id) as total_orders from order_payments
group by payment_type order by total_orders desc;


#Churned customer
create view vw_churned_customers as
select customer_id from orders group by customer_id having count(order_id)=1;



######################################################################################################################################################################
