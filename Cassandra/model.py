#!/usr/bin/env python3
import datetime
import logging
import random
import uuid

import time_uuid
from datetime import datetime, timedelta
from cassandra.query import BatchStatement

# Set logger
log = logging.getLogger()

def segundos():
    return random.randint(1, 360)

def generate_uuid():
    return uuid.uuid4()

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
        session_id timeuuid,
        page_name TEXT,
        entry_time timestamp,
        exit_time timestamp,
        duration_seconds INT,
        PRIMARY KEY ((user_id), session_id)
    ) WITH CLUSTERING ORDER BY (session_id DESC)
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
        product_name TEXT,
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
    purchase_id UUID,
    product_name TEXT,
    quantity INT,   
    price DECIMAL,
    purchase_date timestamp,
    PRIMARY KEY ((user_id), purchase_date, purchase_id)
) WITH CLUSTERING ORDER BY (purchase_date DESC, purchase_id ASC)
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
    PRIMARY KEY ((user_id), category_name)
) WITH CLUSTERING ORDER BY (category_name DESC)
"""

NOTIFICATIONS_CLICKS_BY_USER = """
CREATE TABLE IF NOT EXISTS notifications_clicks_by_user (
    user_id UUID,
    user_name TEXT,
    notification_id UUID,
    click_date timestamp,
    last_session timestamp,
    promotion_type TEXT,
    PRIMARY KEY ((user_id), notification_id)
) WITH CLUSTERING ORDER BY (notification_id DESC)
"""

ERRORS_BY_USER = """
CREATE TABLE IF NOT EXISTS errors_by_user (
    error_id UUID,
    user_name TEXT,
    user_id UUID,
    error_type TEXT,
    error_message TEXT,
    error_date timestamp,
    PRIMARY KEY ((user_id), error_date)
) WITH CLUSTERING ORDER BY (error_date DESC)
"""

PRODUCTS_PROMOTION = """
CREATE TABLE IF NOT EXISTS products_promotion (
    promotion_id UUID,
    product_id INT,
    product_name TEXT,
    promotion_name TEXT,
    promotion_start timestamp,
    promotion_end timestamp,
    PRIMARY KEY ((promotion_id), promotion_start)
) WITH CLUSTERING ORDER BY (promotion_start DESC)
"""

# Q1
SELECT_SEARCH_BY_USER = """
    SELECT user_id, user_name, search_term, category, search_date
    FROM search_history_by_user
    WHERE user_id = ?
    ORDER BY search_date DESC;
"""

# Q2
SELECT_USER_NAVIGATION_SESSION = """
SELECT user_id, user_name, session_id, page_name, duration_seconds, entry_time, exit_time
FROM user_navigation_session
WHERE user_id = ?
ORDER BY session_id DESC;

"""

# Q3
SELECT_BRANDS_VISITED_BY_USER = """
SELECT user_name, brand_name, visit_count, last_visit
FROM brands_visit_by_user 
WHERE user_id = ?
ORDER BY brand_name DESC 
"""

# Q4
SELECT_PRODUCTS_VIEWS_BY_USER = """
SELECT user_name,product_name, product_id, category, view_date
FROM products_views_by_user 
WHERE user_id = ? 
ORDER BY view_date
"""

# Q5
SELECT_PURCHASES_BY_USER = """
SELECT
user_name,
purchase_id,
product_name,
quantity,   
price,
purchase_date
FROM purchases_by_user
WHERE user_id = ?
ORDER BY purchase_date DESC
"""

#Q6
SELECT_AD_CLICKS_BY_USER = """
SELECT user_name, ad_name, action_type, click_date
FROM ad_clicks_by_user
WHERE user_id = ?
ORDER BY click_date DESC
"""

#Q7
SELECT_TIME_SPENT_BY_CATEGORY_BY_USER = """
SELECT user_name, category_name, total_time_seconds, last_session FROM time_spent_by_user
WHERE user_id = ?
ORDER BY category_name DESC
"""

#Q8
SELECT_ERRORS_BY_USER = """
SELECT * FROM errors_by_user
WHERE user_id = ?
ORDER BY error_date DESC
"""

#Q9
SELECT_NOTIFICATIONS_CLICK_BY_USER = """
SELECT user_name, notification_id, click_date, promotion_type FROM notifications_clicks_by_user
WHERE user_id = ?
ORDER BY notification_id DESC
"""

#Q10
SELECT_PRODUCTS_PROMOTION = """
SELECT * FROM products_promotion
WHERE promotion_id = ?
ORDER BY promotion_start DESC
"""

# Sample data
from data.data_cassandra import PRODUCTS,USERS,BRANDS,SEARCH_TERMS,ADS,ACTION_TYPE,ERRORS,PROMOTION_TYPE,USER_IDS

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
    search_history_by_user_stmt = session.prepare("INSERT INTO search_history_by_user (user_id, user_name, search_term, category, search_date) VALUES (?, ?, ?, ?, ?)")
    user_navigation_session_stmt = session.prepare("INSERT INTO user_navigation_session (user_id, user_name, session_id, page_name, entry_time, exit_time, duration_seconds) VALUES (?, ?, ?, ?, ?, ?, ?)")
    brands_visit_by_user_stmt = session.prepare("INSERT INTO brands_visit_by_user (user_id, user_name, brand_name, visit_count, last_visit) VALUES (?, ?, ?, ?, ?)")
    products_views_by_user_stmt = session.prepare("INSERT INTO products_views_by_user (user_id, user_name, product_name, product_id, category, view_date) VALUES (?, ?, ?, ?, ?, ?)")
    purchases_by_user_stmt = session.prepare("INSERT INTO purchases_by_user (user_id, user_name, purchase_id, product_name, quantity, price, purchase_date) VALUES (?, ?, ?, ?, ?, ?, ?)")
    ad_clicks_by_user_stmt = session.prepare("INSERT INTO ad_clicks_by_user (user_id, user_name, ad_name, click_date, action_type) VALUES (?, ?, ?, ?, ?)")
    time_spent_by_user_stmt = session.prepare("INSERT INTO time_spent_by_user (user_id, user_name, category_name, total_time_seconds, last_session) VALUES (?, ?, ?, ?, ?)")
    notifications_clicks_by_user_stmt = session.prepare("INSERT INTO notifications_clicks_by_user (user_id, user_name, notification_id, click_date, last_session, promotion_type) VALUES (?, ?, ?, ?, ?, ?)")
    errors_by_user_stmt = session.prepare("INSERT INTO errors_by_user (error_id, user_name, user_id, error_type, error_message, error_date) VALUES (?, ?, ?, ?, ?, ?)")
    products_promotion_stmt = session.prepare("INSERT INTO products_promotion (promotion_id, product_id, product_name, promotion_name, promotion_start, promotion_end) VALUES (?, ?, ?, ?, ?, ?)")

    # INSERT STATEMENTS
    
    history_data = []
    navigation_data = []
    brands_data = []
    products_views_data = []
    purchases_data = []
    ad_clicks_data = []
    time_spent_data = []    
    notifications_data = []
    errors_data =[]
    promotion_data =[]

    #para llenar search_history_by_user
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(2, 5)):
            term, cat = random.choice(SEARCH_TERMS)
            date = random_date(2023, 2025)
            history_data.append((user_id, user_name, term, cat, date))

    #para llenar user_navigation_session
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(2, 5)):
            session_id = generar_timeuuid()
            page_name = random.choice(BRANDS)
            entry = random_date(2023, 2025)
            exit = entry + timedelta(days=random.randint(1, 30))
            seconds = segundos()
            navigation_data.append((user_id,user_name,session_id,page_name,entry,exit,seconds))

    #para llenar brands_visit_by_user
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(2, 5)):
            brand_name = random.choice(BRANDS)
            visit_count = random.randint(1, 15)
            last = random_date(2023, 2025)
            brands_data.append((user_id,user_name,brand_name,visit_count,last))

    #para llenar products_views
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(2, 5)):
            cat, p_name, price = random.choice(PRODUCTS)
            product_id = random.randint(1, 999)
            view_date = random_date(2023, 2025)
            products_views_data.append((user_id,user_name,p_name,product_id,cat,view_date))

    #para llenar purchase_by_user
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(2, 5)):
            purchase_id = generate_uuid()
            cat, p_name, price = random.choice(PRODUCTS)
            quantity = random.randint(1, 3)
            product_id = random.randint(1, 999)
            date = random_date(2023, 2025)
            purchases_data.append((user_id,user_name,purchase_id,p_name,quantity,price, date))

    #para llenar ad_clicks_by_user
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(2, 5)):
            ad_name = random.choice(ADS)
            date = random_date(2023, 2025)
            action_type = random.choice(ACTION_TYPE)
            ad_clicks_data.append((user_id,user_name,ad_name,date,action_type))

    #para llenar time_spent_by_category
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for category in [t[1] for t in SEARCH_TERMS]:
            total_time = segundos()
            last_session = random_date(2023, 2025)
            time_spent_data.append((user_id, user_name, category, total_time, last_session))
    
    #para llenar notifications click
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(1, 3)):
            notification_id = generate_uuid()
            click_date = random_date(2023, 2025)
            last_session = random_date(2023, 2025)
            promotion_type = random.choice(PROMOTION_TYPE)
            notifications_data.append((user_id,user_name,notification_id,click_date,last_session,promotion_type))

    #para llenar erros_by_user
    for user_name in USERS:
        user_id = USER_IDS[user_name]
        for i in range(random.randint(1, 3)):
            error_id = generate_uuid()
            error_type, error_msg = random.choice(ERRORS)
            error_date = random_date(2023, 2025)
            errors_data.append((error_id,user_name,user_id,error_type,error_msg,error_date))

    #para llenar products_promotion
    for i in range(random.randint(1, 8)):
        promotion_id =generate_uuid()
        product_id = random.randint(1, 999)
        cat, product_name, pr = random.choice(PRODUCTS)
        promotion_name = random.choice(PROMOTION_TYPE)
        promotion_start = random_date(2023, 2025)
        promotion_end = promotion_start + timedelta(days=random.randint(1, 30))
        promotion_data.append((promotion_id,product_id,product_name,promotion_name,promotion_start,promotion_end))

    
    #añadimos a las tablas 
    execute_batch(session,search_history_by_user_stmt, history_data)
    execute_batch(session,user_navigation_session_stmt,navigation_data)
    execute_batch(session,brands_visit_by_user_stmt,brands_data)
    execute_batch(session,products_views_by_user_stmt,products_views_data)
    execute_batch(session,purchases_by_user_stmt,purchases_data)
    execute_batch(session,ad_clicks_by_user_stmt,ad_clicks_data)
    execute_batch(session,time_spent_by_user_stmt,time_spent_data)
    execute_batch(session,notifications_clicks_by_user_stmt,notifications_data)
    execute_batch(session,errors_by_user_stmt,errors_data)
    execute_batch(session,products_promotion_stmt,promotion_data)


def random_date(start_f, end_f):
    start = datetime(start_f, 1, 1)
    end = datetime(end_f, 12, 31)

    diff = end - start
    random_seconds = random.randint(0, int(diff.total_seconds()))

    return start + timedelta(seconds=random_seconds)

def generar_timeuuid():
    return uuid.uuid1()

def create_keyspace(session, keyspace, replication_factor):
    log.info(f"Creating keyspace: {keyspace} with replication factor {replication_factor}")
    session.execute(CREATE_KEYSPACE.format(keyspace, replication_factor))

def create_schema(session):
    log.info("Creating logistics schema")
    session.execute(SEARCH_HISTORY_BY_USER)
    session.execute(USER_NAVIGATION_SESSION)
    session.execute(BRANDS_VISIT_BY_USER)
    session.execute(PRODUCTS_VIEWS_BY_USER)
    session.execute(PURCHASES_BY_USER)
    session.execute(AD_CLICKS_BY_USER)
    session.execute(TIME_SPENT_BY_CATEGORY_BY_USER)
    session.execute(NOTIFICATIONS_CLICKS_BY_USER)
    session.execute(ERRORS_BY_USER)
    session.execute(PRODUCTS_PROMOTION)



#queries
# Q1: select_searches_by_user
def searches_by_user(session, user_id):

    stmt = session.prepare(SELECT_SEARCH_BY_USER)
    rows = session.execute(stmt, [user_id])
    
    for row in rows:
        print(f"\n=== Search by user: {row.user_name} ===")
        print(f"User_name: {row.user_name}")
        print(f"Search term: {row.search_term}")
        print(f"Category: {row.category}")
        print(f"Search date: {row.search_date}")
        print()

# Q2: select_user_navigation_sessions
def user_navigation_sessions(session, user_id):
    stmt = session.prepare(SELECT_USER_NAVIGATION_SESSION)
    rows = session.execute(stmt,([user_id]))

    for row in rows:
        print(f"\n=========User {row.user_name} navigation session==============")
        print(f'user_name: {row.user_name}')
        print(f'Session: {row.session_id}')
        print(f'Page name: {row.page_name}')
        print(f'Duration: {row.duration_seconds} seconds')
        print("===============================\n")

#Q3 marcas visitadas por usuario
def brands_searchs_by_user(session, user_id):
    stmt = session.prepare(SELECT_BRANDS_VISITED_BY_USER)
    rows = session.execute(stmt, ([user_id]))

    for row in rows:
        print(f"\n=========brands visited by {row.user_name}==============")
        print(f'User name: {row.user_name}')
        print(f'Brand name: {row.brand_name}')
        print(f'Visits count: {row.visit_count}')
        print(f'last visit: {row.last_visit} ')
        print("===============================\n")

#Q4
def products_views_by_user(session, user_id):
    stmt = session.prepare(SELECT_PRODUCTS_VIEWS_BY_USER)
    rows = session.execute(stmt, ([user_id]))

    for row in rows:
        print(f"\n=========Products views by {row.user_name}==============")
        print(f'User name: {row.user_name}')
        print(f'Product name: {row.product_name}')
        print(f'Product id: {row.product_id}')
        print(f'Category: {row.category} ')
        print(f'View date: {row.view_date}')
        print("===============================\n")

#Q5
def purchases_by_user(session, user_id):
    stmt = session.prepare(SELECT_PURCHASES_BY_USER)
    rows = session.execute(stmt, ([user_id]))

    for row in rows:
        print(f"\n=========Purchases by {row.user_name}==============")
        print(f'User name: {row.user_name}')
        print(f'Purchase id: {row.purchase_id}')
        print(f'Product name: {row.product_name}')
        print(f'Quantity: {row.quantity} ')
        print(f'Price: {row.price:.2f}')
        print(f'Total: {(row.quantity * row.price):.2f}')
        print(f'Purchase date: {row.purchase_date}')
        print("===============================\n")

#Q6
def clicked_ads_by_user(session, user_id):
    stmt = session.prepare(SELECT_AD_CLICKS_BY_USER)
    rows = session.execute(stmt, ([user_id]))

    for row in rows:
        print(f"\n=========Ad's clicked by {row.user_name}==============")
        print(f'User name: {row.user_name}')
        print(f'Ad name: {row.ad_name}')
        print(f'Click date: {row.click_date}')
        print(f'Action Type: {row.action_type}')
        print("===============================\n")

#Q7
def time_per_category_by_user(session, user_id):
    stmt = session.prepare(SELECT_TIME_SPENT_BY_CATEGORY_BY_USER)
    rows = session.execute(stmt, ([user_id]))

    for row in rows:
        print(f"\n=========Time by category by {row.user_name}==============")
        print(f'User name: {row.user_name}')
        print(f'Category name: {row.category_name}')
        print(f'Seconds spend: {row.total_time_seconds}')
        print(f'Last session: {row.last_session}')
        print("===============================\n")  

#Q8
def clicks_notifications_by_user(session, user_id):
    stmt = session.prepare(SELECT_NOTIFICATIONS_CLICK_BY_USER)
    rows = session.execute(stmt, ([user_id]))

    for row in rows:
        print(f"\n=========Notifications clicked by {row.user_name}==============")
        print(f'User name: {row.user_name}')
        print(f'Notification id: {row.notification_id}')
        print(f'Promotion type: {row.promotion_type}')
        print(f'Click date: {row.click_date}')
        print("===============================\n")   

#Q9
def errors_by_user(session, user_id):
    stmt = session.prepare(SELECT_ERRORS_BY_USER)
    rows = session.execute(stmt, ([user_id]))

    for row in rows:
        print(f"\n=========Errors in {row.user_name} session==============")
        print(f'Error id: {row.error_id}')
        print(f'Error type: {row.error_type}')
        print(f'Error message: {row.error_message}')
        print(f'Error date: {row.error_date}')
        print("===============================\n") 

#Q10
def promotions_by_user(session, promotion_id):
    stmt = session.prepare(SELECT_PRODUCTS_PROMOTION)
    rows = session.execute(stmt, ([promotion_id]))

    for row in rows:
        print(f"\n=========Info of promotion {row.promotion_name}==============")
        print(f'Promotion id: {row.promotion_id}')
        print(f'Product id: {row.product_id}')
        print(f'Product name: {row.product_name}')
        print(f'Promotion start: {row.promotion_start}')
        print(f'Promotion end: {row.promotion_end}')
        print("===============================\n")       