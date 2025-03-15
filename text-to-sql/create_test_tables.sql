-- Create a customer touchpoints table
CREATE OR REPLACE TABLE "PROD".customer.customer_touchpoints (
    touchpoint_id NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    campaign_id NUMBER,
    touchpoint_source VARCHAR(50),
    touchpoint_type VARCHAR(50),
    interaction_date TIMESTAMP_NTZ,
    interaction_value FLOAT,
    interaction_description VARCHAR(255),
    device_type VARCHAR(50),
    location VARCHAR(100)
);

-- Insert sample data
INSERT INTO "PROD".customer.customer_touchpoints (
    touchpoint_id,
    customer_id,
    product_id,
    campaign_id,
    touchpoint_source,
    touchpoint_type,
    interaction_date,
    interaction_value,
    interaction_description,
    device_type,
    location
) VALUES 
    (1, 101, 1, 1, 'website', 'page_visit', '2024-03-15 10:30:00', 0, 'Visited product page', 'desktop', 'New York, USA'),
    (2, 101, 1, 1, 'email', 'open', '2024-03-16 14:20:00', 0, 'Opened promotional email', 'mobile', 'New York, USA'),
    (3, 101, 1, 1, 'store', 'purchase', '2024-03-17 16:45:00', 299.99, 'In-store purchase', 'pos', 'NYC Store'),
    (4, 102, 3, 2, 'mobile_app', 'login', '2024-03-15 09:15:00', 0, 'Mobile app login', 'mobile', 'Los Angeles, USA'),
    (5, 102, 3, 2, 'customer_service', 'call', '2024-03-15 11:30:00', 0, 'Support call about product', 'phone', 'Support Center'),
    (6, 103, 4, 2, 'social_media', 'engagement', '2024-03-16 13:00:00', 0, 'Liked company post', 'mobile', 'Chicago, USA'),
    (7, 103, 4, 2, 'website', 'cart_add', '2024-03-16 13:30:00', 149.99, 'Added item to cart', 'tablet', 'Chicago, USA'),
    (8, 103, 4, 2, 'email', 'click', '2024-03-17 10:00:00', 0, 'Clicked email link', 'desktop', 'Chicago, USA'),
    (9, 104, 2, 3, 'store', 'visit', '2024-03-15 15:20:00', 0, 'Store visit', 'pos', 'Houston Store'),
    (10, 104, 2, 3, 'website', 'purchase', '2024-03-16 20:15:00', 199.99, 'Online purchase', 'desktop', 'Houston, USA');

-- Create a customer profile table
CREATE OR REPLACE TABLE "PROD".customer.customer_profiles (
    customer_id NUMBER,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    email VARCHAR(100),
    phone VARCHAR(20),
    signup_date DATE,
    customer_segment VARCHAR(50),
    birth_date DATE,
    city VARCHAR(50),
    country VARCHAR(50),
    lifetime_value NUMBER(10,2)
);

-- Insert sample customer profile data
INSERT INTO "PROD".customer.customer_profiles (
    customer_id,
    first_name,
    last_name,
    email,
    phone,
    signup_date,
    customer_segment,
    birth_date,
    city,
    country,
    lifetime_value
) VALUES 
    (101, 'John', 'Doe', 'john.doe@email.com', '+1-555-0101', '2024-01-15', 'Premium', '1985-05-15', 'New York', 'USA', 2500.00),
    (102, 'Jane', 'Smith', 'jane.smith@email.com', '+1-555-0102', '2024-01-20', 'Standard', '1990-08-22', 'Los Angeles', 'USA', 1200.00),
    (103, 'Bob', 'Johnson', 'bob.johnson@email.com', '+1-555-0103', '2024-02-01', 'Premium', '1988-03-30', 'Chicago', 'USA', 3500.00),
    (104, 'Alice', 'Brown', 'alice.brown@email.com', '+1-555-0104', '2024-02-15', 'Standard', '1992-11-08', 'Houston', 'USA', 800.00);

-- Create product categories table
CREATE OR REPLACE TABLE "PROD".product.product_categories (
    category_id NUMBER,
    category_name VARCHAR(50),
    parent_category_id NUMBER,
    category_description VARCHAR(255)
);

INSERT INTO "PROD".product.product_categories VALUES
    (1, 'Electronics', NULL, 'Electronic devices and accessories'),
    (2, 'Smartphones', 1, 'Mobile phones and accessories'),
    (3, 'Laptops', 1, 'Portable computers'),
    (4, 'Fashion', NULL, 'Clothing and accessories'),
    (5, 'Men''s Wear', 4, 'Clothing for men');

-- Create products table
CREATE OR REPLACE TABLE "PROD".product.products (
    product_id NUMBER,
    category_id NUMBER,
    product_name VARCHAR(100),
    price NUMBER(10,2),
    stock_level NUMBER,
    launch_date DATE
);

INSERT INTO "PROD".product.products VALUES
    (1, 2, 'Premium Smartphone X', 999.99, 50, '2024-01-01'),
    (2, 2, 'Smart Watch Pro', 299.99, 100, '2024-01-15'),
    (3, 3, 'Ultrabook Pro', 1499.99, 30, '2024-02-01'),
    (4, 5, 'Designer Jacket', 199.99, 75, '2024-02-15');

-- Create marketing_campaigns table
CREATE OR REPLACE TABLE "PROD".marketing.marketing_campaigns (
    campaign_id NUMBER,
    campaign_name VARCHAR(100),
    campaign_type VARCHAR(50),
    start_date DATE,
    end_date DATE,
    budget NUMBER(10,2),
    target_segment VARCHAR(50)
);

INSERT INTO "PROD".marketing.marketing_campaigns VALUES
    (1, 'Spring Sale 2024', 'Seasonal', '2024-03-01', '2024-03-31', 50000.00, 'All'),
    (2, 'Premium Customer Appreciation', 'Loyalty', '2024-03-15', '2024-04-15', 25000.00, 'Premium'),
    (3, 'New Customer Welcome', 'Acquisition', '2024-03-10', '2024-04-10', 30000.00, 'Standard');

-- Create customer_feedback table
CREATE OR REPLACE TABLE "PROD".customer.customer_feedback (
    feedback_id NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    rating NUMBER,
    feedback_text VARCHAR(500),
    feedback_date TIMESTAMP_NTZ,
    sentiment VARCHAR(20)
);

INSERT INTO "PROD".customer.customer_feedback VALUES
    (1, 101, 1, 5, 'Great product, excellent features!', '2024-03-16 15:30:00', 'Positive'),
    (2, 102, 2, 3, 'Average performance, could be better', '2024-03-17 11:20:00', 'Neutral'),
    (3, 103, 3, 4, 'Good value for money', '2024-03-18 14:45:00', 'Positive');

-- Create transactions table
CREATE OR REPLACE TABLE "PROD".transaction.transactions (
    transaction_id NUMBER,
    customer_id NUMBER,
    product_id NUMBER,
    campaign_id NUMBER,
    transaction_date TIMESTAMP_NTZ,
    quantity NUMBER,
    total_amount NUMBER(10,2),
    payment_method VARCHAR(50),
    status VARCHAR(20)
);

INSERT INTO "PROD".transaction.transactions VALUES
    (1, 101, 1, 1, '2024-03-15 10:45:00', 1, 999.99, 'Credit Card', 'Completed'),
    (2, 101, 2, 1, '2024-03-15 10:45:00', 1, 299.99, 'Credit Card', 'Completed'),
    (3, 102, 3, 2, '2024-03-16 14:30:00', 1, 1499.99, 'Debit Card', 'Completed'),
    (4, 103, 4, 2, '2024-03-17 16:20:00', 2, 399.98, 'PayPal', 'Completed'); 