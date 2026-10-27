CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS audit;

CREATE TABLE IF NOT EXISTS gold.dim_customers (
    customer_key            INT PRIMARY KEY,
    customer_id             INT,
    customer_number         VARCHAR(50),
    first_name              VARCHAR(50), 
    last_name	            VARCHAR(50),
    country	                VARCHAR(50),
    marital_status	        VARCHAR(50),
    gender	                VARCHAR(50),
    birthdate	            DATE,
    create_date	            DATE
);

CREATE TABLE IF NOT EXISTS gold.dim_products (
    product_key	            INT PRIMARY KEY,
    product_id	            INT,
    product_number	        VARCHAR(50),
    product_name	        VARCHAR(255),
    category_id	            VARCHAR(50),
    category                VARCHAR(50), 
    subcategory	            VARCHAR(50),
    maintenance	            VARCHAR(50),
    cost	                INT,
    product_line	        VARCHAR(50),
    start_date	            DATE
);

CREATE TABLE IF NOT EXISTS gold.fact_sales (
    order_number            VARCHAR(50),
    product_key             INT,
    customer_key            INT,
    order_date              DATE,
    shipping_date           DATE,
    due_date                DATE,
    sales_amount            INT,
    quantity                INT,
    price                   INT,
    CONSTRAINT fk_dim_customers
    FOREIGN KEY (customer_key)
    REFERENCES gold.dim_customers(customer_key),
    CONSTRAINT fk_dim_products
    FOREIGN KEY (product_key)
    REFERENCES gold.dim_products(product_key)
);