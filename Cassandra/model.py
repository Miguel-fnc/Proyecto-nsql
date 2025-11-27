#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

import time_uuid
from cassandra.query import BatchStatement

# Set logger
log = logging.getLogger()

CREATE_KEYSPACE = """
        CREATE KEYSPACE IF NOT EXISTS {}
        WITH replication = {{ 'class': 'SimpleStrategy', 'replication_factor': {} }}
"""

SEARCH_HISTORY_BY_USER = """
    CREATE TABLE IF NOT EXISTS search_history_by_user (
        user_id UUID,
        user_name TEXT,
        search_term TEXT,
        category TEXT,
        search_date timestamp,
        PRIMARY KEY ((user_id), search_date)
    ) WITH CLUSTERING ORDER BY (search_date DESC)
"""

USER_NAVIGATION_SESSION = """
    CREATE TABLE IF NOT EXISTS user_navigation_session (
        user_id UUID,
        user_name TEXT,
        session_id INT,
        page_name TEXT,
        entry_time timestamp,
        exit_time timestamp,
        duration_seconds INT,
        PRIMARY KEY ((user_id), duration_seconds)
    ) WITH CLUSTERING ORDER BY (duration_seconds DESC)
"""

BRANDS_VISIT_BY_USER = """
    CREATE TABLE IF NOT EXISTS brands_visit_by_user (
        user_id UUID,
        user_name TEXT,
        brand_name TEXT,
        visit_count INT,
        last_visit timestamp,
        PRIMARY KEY ((user_id), brand_name)
    ) WITH CLUSTERING ORDER BY (brand_name DESC)
"""

PRODUCTS_VIEWS_BY_USER = """
    CREATE TABLE IF NOT EXISTS products_views_by_user (
        user_id UUID,
        user_name TEXT,
        product_id INT,
        category TEXT,
        view_date timestamp,
        PRIMARY KEY ((user_id), view_date)
    ) WITH CLUSTERING ORDER BY (view_date DESC)
"""

PURCHASES_BY_USER = """
CREATE TABLE IF NOT EXISTS purchases_by_user (
    user_id UUID,
    user_name TEXT,
    purchase_id INT,
    product_name INT,
    quantity INT,   
    price DECIMAL,
    purchase_date timestamp,
    PRIMARY KEY ((user_id), purchase_date)
) WITH CLUSTERING ORDER BY (purchase_date DESC)
"""

AD_CLICKS_BY_USER = """
CREATE TABLE IF NOT EXISTS ad_clicks_by_user (
    user_id UUID,
    user_name TEXT,
    ad_name TEXT,
    click_date timestamp,
    action_type TEXT,
    PRIMARY KEY ((user_id), click_date)
) WITH CLUSTERING ORDER BY (click_date DESC)
"""

TIME_SPENT_BY_CATEGORY_BY_USER = """
CREATE TABLE IF NOT EXISTS time_spent_by_user (
    user_id UUID,
    user_name TEXT,
    category_name TEXT,
    total_time_seconds INT,
    last_session timestamp,
    PRIMARY KEY ((user_id), total_time_seconds)
) WITH CLUSTERING ORDER BY (total_time_seconds DESC)
"""

NOTIFICATIONS_CLICKS_BY_USER = """
CREATE TABLE IF NOT EXISTS notifications_clicks_by_user (
    user_id UUID,
    user_name TEXT,
    notification_id INT,
    click_date timestamp,
    last_session timestamp,
    promotion_type TEXT,
    PRIMARY KEY ((user_id), click_date)
) WITH CLUSTERING ORDER BY (click_date DESC)
"""

ERRORS_BY_USER = """
CREATE TABLE IF NOT EXISTS errors_by_user (
    error_id INT,
    user_name TEXT,
    user_id UUID,
    error_type TEXT,
    error_message TEXT,
    error_date timestamp,
    PRIMARY KEY ((user_id), error_type)
) WITH CLUSTERING ORDER BY (error_type DESC)
"""

PRODUCTS_PROMOTION = """
CREATE TABLE IF NOT EXISTS products_promotion (
    promotion_id UUID,
    product_id INT,
    promotion_name TEXT,
    promotion_start timestamp,
    promotion_end timestamp,
    PRIMARY KEY ((promotion_id), promotion_start, promotion_end)
) WITH CLUSTERING ORDER BY (promotion_start ASC, promotion_end DESC)
"""

# Q1
SELECT_SEARCH_BY_USER = """
    SELECT user_id, user_name, search_term, category name, dateOf(search_date) as search_on
    FROM search_history_by_user
    WHERE user_id = ?
    ORDER BY search_on DESC;
"""

# Q2
SELECT_USER_NAVIGATION_SESSION = """
SELECT user_id, user_name, page_name, duration_seconds
FROM user_navigation_session
WHERE user_id = ?
ORDER BY duration_seconds DESC;

"""

# Q3
SELECT_BRANDS_VISITED_BY_USER = """
SELECT
user_id,
user_name,
brand_name,
visit_count,
dateOf(last_visit) as visited_on
FROM brand_visits_by_user 
WHERE user_id = ?;  
ORDER BY brand_name DESC 
"""

# Q4
SELECT_PRODUCTS_VIEWS_BY_USER = """
SELECT user_id, user_name, category, dateOf(view_date) as view_on
FROM products_views_by_user 
WHERE user_id = ? 
ORDER_BY view_date
"""

# Q5
SELCT_PURCHASES_BY_USER = """
SELECT
user_id,
user_name,
purchase_id,
product_name,
quantity,   
price,
dateOf(purchase_date) as purchase_on,
FROM purchases_by_user
WHERE user_id = ?
ORDER BY purchase_on DESC
"""

# Q6
SELECT_AD_CLICKS_BY_USER = """
SELECT user_id, user_name, click_date
FROM ad_clicks_by_user
WHERE user_id = ?
ORDER BY click_date DESC
"""
# Q7
SELECT_TIME_SPENT_BY_CATEGORY_BY_USER = """
SELECT * FROM time_spent_by_category_by_user
WHERE user_id = ?
ORDER BY total_time_seconds DESC
"""
#Q8
SELECT_ERRORS_BY_USER = """
SELECT * FROM errors_by_user
WHERE user_id = ?
ORDER BY error_date DESC
"""
#Q9
SELECT_NOTIFICATIONS_CLICK_BY_USER = """
SELECT * FROM notification_clicks_by_user
WHERE user_id = ?
ORDER BY click_date DESC
"""

#Q10
SELECT_PRODUCTS_PROMOTION = """
SELECT 
promotion_name,
promotion_id,
product_id,
dateOf(promotion_start) as start_date,
dateOf(promotion_end) as end_date,
clicks_total
WHERE promotion_id = ?
ORDER BY end_date DESC
"""

# Sample data
CUSTOMERS = [
    ('juan.perez@email.com', 'Juan Pérez', '+52-33-1234-5678', 'Av. Patria 1234, Zapopan, Jalisco'),
    ('maria.gonzalez@email.com', 'María González', '+52-33-2345-6789', 'Calle Independencia 567, Guadalajara, Jalisco'),
    ('carlos.rodriguez@email.com', 'Carlos Rodríguez', '+52-33-3456-7890', 'Av. Américas 890, Guadalajara, Jalisco'),
    ('ana.martinez@email.com', 'Ana Martínez', '+52-33-4567-8901', 'Calle Libertad 123, Tlaquepaque, Jalisco'),
    ('luis.lopez@email.com', 'Luis López', '+52-33-5678-9012', 'Av. López Mateos 456, Zapopan, Jalisco'),
    ('sofia.hernandez@email.com', 'Sofía Hernández', '+52-33-6789-0123', 'Calle Reforma 789, Guadalajara, Jalisco')
]

PRODUCTS = [
    ('Laptop Dell XPS 13', 'Electrónicos', 25000.00),
    ('iPhone 14 Pro', 'Electrónicos', 28000.00),
    ('Samsung Galaxy S23', 'Electrónicos', 22000.00),
    ('Nike Air Max 270', 'Calzado', 3500.00),
    ('Adidas Ultraboost 22', 'Calzado', 4200.00),
    ('Levi\'s 501 Jeans', 'Ropa', 1800.00),
    ('Camisa Hugo Boss', 'Ropa', 2500.00),
    ('Mochila North Face', 'Accesorios', 1200.00),
    ('Reloj Apple Watch Series 8', 'Electrónicos', 8500.00),
    ('Audífonos Sony WH-1000XM4', 'Electrónicos', 6500.00),
    ('Cafetera Nespresso', 'Hogar', 3200.00),
    ('Aspiradora Dyson V11', 'Hogar', 12000.00),
    ('Licuadora Vitamix', 'Hogar', 8000.00),
    ('Perfume Chanel No. 5', 'Belleza', 4500.00),
    ('Crema La Mer', 'Belleza', 6000.00)
]

ORDER_STATUSES = ['Pending', 'Processing', 'Shipped', 'Delivered', 'Cancelled']
SHIPMENT_STATUSES = ['Pending', 'Shipped', 'In Transit', 'Out for Delivery', 'Delivered', 'Delayed', 'Returned']
SHIPMENT_TYPES = ['Standard', 'Express', 'Same-day']

# Get date range from user input or use default (last 30 days)
# Default to last 30 days if not provided
def get_date_range():
    today = datetime.datetime.today()

    bottom_range = input("Type bottom range example (YYYY-MM-DD): ").strip()
    top_range = input("Type top range example (YYYY-MM-DD): ").strip()
    if bottom_range == "" and top_range == "":
        bottom_range = today - datetime.timedelta(days=30)
        top_range = today

    elif bottom_range == "" and top_range != "":
        top_range = datetime.datetime.strptime(top_range, "%Y-%m-%d")
        bottom_range = top_range - datetime.timedelta(days=30)

    elif top_range == "" and bottom_range != "":
        bottom_range = datetime.datetime.strptime(bottom_range, "%Y-%m-%d")
        top_range = bottom_range + datetime.timedelta(days=30)

    else:
        bottom_range = datetime.datetime.strptime(bottom_range, "%Y-%m-%d")
        top_range = datetime.datetime.strptime(top_range, "%Y-%m-%d")

    return bottom_range, top_range


def execute_batch(session, stmt, data):
    batch_size = 10
    for i in range(0, len(data), batch_size):
        batch = BatchStatement()
        for item in data[i : i+batch_size]:
            batch.add(stmt, item)
        session.execute(batch)

def bulk_insert(session):
    # Prepare statements
    orders_stmt = session.prepare("INSERT INTO orders_by_customers (email, order_date, name, order_number, total_amount, status) VALUES (?, ?, ?, ?, ?, ?)")
    products_stmt = session.prepare("INSERT INTO products_by_order (order_number, price, product_name, category, quantity) VALUES (?, ?, ?, ?, ?)")
    shipments_sd_stmt = session.prepare("INSERT INTO shipments_by_o_sd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    shipments_ssd_stmt = session.prepare("INSERT INTO shipments_by_o_ssd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    shipments_tsd_stmt = session.prepare("INSERT INTO shipments_by_o_tsd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)")
    shipments_tssd_stmt = session.prepare("INSERT INTO shipments_by_o_tssd (order_number, shipment_date, tracking_number, status, type, total_amount, customer_name) VALUES (?, ?, ?, ?, ?, ?, ?)")

    # INSERT STATEMENTS
    
    orders_num = 100
    products_per_order = 3
    shipments_per_order = 10
    
    orders_data = []
    products_data = []
    shipments_data = []
    shipments_ssd_data = []
    shipments_tsd_data = []
    shipments_tssd_data = []





    # Generate orders
    for i in range(orders_num):
        customer = random.choice(CUSTOMERS)
        order_number = f"ORD-{uuid.uuid4().hex[:8].upper()}"
        order_date = random_date(datetime.datetime(2024, 1, 1), datetime.datetime(2025, 12, 31))
        total_amount = 0
        
        # Generate products for this order
        selected_products = random.sample(PRODUCTS, products_per_order)
        for product_name, category, price in selected_products:
            quantity = random.randint(1, 3)
            total_amount += price * quantity
            products_data.append((order_number, price, product_name, category, quantity))
        
        status = random.choice(ORDER_STATUSES)
        orders_data.append((customer[0], order_date, customer[1], order_number, total_amount, status))
        
        # Generate shipments for this order
        for j in range(shipments_per_order):
            tracking_number = f"TRK-{uuid.uuid4().hex[:10].upper()}"
            shipment_date = random_date(datetime.datetime(2024, 1, 1), datetime.datetime(2025, 12, 31))
            ship_status = random.choice(SHIPMENT_STATUSES)
            ship_type = random.choice(SHIPMENT_TYPES)
            ship_amount = total_amount / shipments_per_order
            
            # Add to all shipment tables
            shipments_data.append((order_number, shipment_date, tracking_number, ship_status, ship_type, ship_amount, customer[1]))
            shipments_ssd_data.append((order_number, shipment_date, tracking_number, ship_status, ship_type, ship_amount, customer[1]))
            shipments_tsd_data.append((order_number, shipment_date, tracking_number, ship_status, ship_type, ship_amount, customer[1]))
            shipments_tssd_data.append((order_number, shipment_date, tracking_number, ship_status, ship_type, ship_amount, customer[1]))

    # Execute batch inserts
    execute_batch(session, orders_stmt, orders_data)
    execute_batch(session, products_stmt, products_data)
    
    # Insert into all shipment tables (same data, same order)
    execute_batch(session, shipments_sd_stmt, shipments_data)
    # ADD EXECUTE STATEMENTS
    execute_batch(session, shipments_ssd_stmt, shipments_ssd_data)
    execute_batch(session,shipments_tsd_stmt, shipments_tsd_data)
    execute_batch(session, shipments_tssd_stmt, shipments_tssd_data)

def random_date(start_date, end_date):
    """Generate a random date between start_date and end_date"""
    time_between_dates = end_date - start_date
    days_between_dates = time_between_dates.days
    random_number_of_days = random.randrange(days_between_dates)
    rand_date = start_date + datetime.timedelta(days=random_number_of_days)
    return time_uuid.TimeUUID.with_timestamp(time_uuid.mkutime(rand_date))

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creating logistics schema")
    session.execute(CREATE_ORDERS_BY_CUSTOMERS_TABLE)
    session.execute(CREATE_PRODUCTS_BY_ORDER_TABLE)
    session.execute(CREATE_SHIPMENTS_BY_O_SD_TABLE)
    # ADD CREATE TABLES
    session.execute(CREATE_SHIPMENTS_BY_O_SSD_TABLE)
    session.execute(CREATE_SHIPMENTS_BY_O_TSD_TABLE)
    session.execute(CREATE_SHIPMENTS_BY_O_TSSD_TABLE)

# Q1: Get orders by customer
def get_orders_by_customer(session, email):

    stmt = session.prepare(SELECT_ORDERS_BY_CUSTOMER)
    rows = session.execute(stmt, [email])
    
    print(f"\n=== Orders for customer: {email} ===")
    for row in rows:
        print(f"Order: {row.order_number}")
        print(f"  - Date: {row.order_date_readable}")
        print(f"  - Customer: {row.name}")
        print(f"  - Total: ${row.total_amount:,.2f}")
        print(f"  - Status: {row.status}")
        print()

# Q2: Get products by order
def get_products_by_order(session, order_number):
    stmt = session.prepare(PRODUCTS_BY_ORDER)
    rows = session.execute(stmt,([order_number]))

    for row in rows:
        print("===============================")
        print(f'Product name: {row.product_name}')
        print(f'Order number: {row.order_number}')
        print(f'Order Price: ${row.price}')
        print(f'Order category: {row.category}')
        print(f'Quantity: {row.quantity}')
        print("===============================\n")

# Q3.1: Get all shipments by order (no date filter)
def all_shipments(session,order_number):
    stmt = session.prepare(ALL_SHIPMENTS)
    rows = session.execute(stmt,([order_number]))
    for row in rows:
        print("===============================")
        print(f'Customer name: {row.customer_name}')
        print(f'Order number: {row.order_number}')
        print(f'Shipment date: {row.date}')
        print(f'Shipment status: {row.status}')
        print(f'Total amount: {row.total_amount}')
        print(f'Tracking number: {row.tracking_number}')
        print(f'Delivery type: {row.type}')
        print("===============================\n")
        
# Q3.2: Same as Q3.1 (with explicit date range)
def all_shipments_range(session, order_number, bottom_range, top_range):
    stmt = session.prepare(ALL_SHIPMENTS_RANGE)
    rows = session.execute(stmt,(order_number,bottom_range,top_range))
    for row in rows:
        print("===============================")
        print(f'Customer name: {row.customer_name}')
        print(f'Order number: {row.order_number}')
        print(f'Shipment date: {row.date}')
        print(f'Shipment status: {row.status}')
        print(f'Total amount: {row.total_amount}')
        print(f'Tracking number: {row.tracking_number}')
        print(f'Delivery type: {row.type}')
        print("===============================\n")
     
# Q3.3: Get shipments by order and status with date range
def all_shipments_range_status(session,order_number,status,bottom_range, top_range):
    stmt = session.prepare(ALL_SHIPMENTS_RANGE_BY_STATUS)
    rows = session.execute(stmt,[order_number,status,bottom_range,top_range])
    for row in rows:
        print("===============================")
        print(f'Customer name: {row.customer_name}')
        print(f'Order number: {row.order_number}')
        print(f'Shipment date: {row.date}')
        print(f'Shipment status: {row.status}')
        print(f'Total amount: {row.total_amount}')
        print(f'Tracking number: {row.tracking_number}')
        print(f'Delivery type: {row.type}')
        print("===============================\n")

# Q3.4: Get shipments by order and type with date range
def all_shipments_order_type_range(session,type, order_number,bottom_range,top_range):
    stmt = session.prepare(ALL_SHIPMENTS_BY_TYPE)
    rows = session.execute(stmt,[type,order_number,bottom_range,top_range])
    for row in rows:
        print("===============================")
        print(f'Customer name: {row.customer_name}')
        print(f'Order number: {row.order_number}')
        print(f'Shipment date: {row.date}')
        print(f'Shipment status: {row.status}')
        print(f'Total amount: {row.total_amount}')
        print(f'Tracking number: {row.tracking_number}')
        print(f'Delivery type: {row.type}')
        print("===============================\n")

# Q3.5: Get shipments by order, type and status with date range
def all_shipments_order_tssd(session, order_number,type,status,bottom_range,top_range):
    stmt = session.prepare(ALL_SHIPMENTS_BY_TSSD)
    rows = session.execute(stmt,[order_number,type,status,bottom_range,top_range])
    for row in rows:
        print("===============================")
        print(f'Customer name: {row.customer_name}')
        print(f'Order number: {row.order_number}')
        print(f'Shipment date: {row.date}')
        print(f'Shipment status: {row.status}')
        print(f'Total amount: {row.total_amount}')
        print(f'Tracking number: {row.tracking_number}')
        print(f'Delivery type: {row.type}')
        print("===============================\n")

