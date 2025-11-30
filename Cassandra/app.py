#!/usr/bin/env python3
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import uuid
from cassandra.cluster import Cluster
import model
from connect import get_cassandra_session
from data.data_cassandra import USER_IDS, PROMOTIONS_IDS

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'ecommerce')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')


def print_menu():
    mm_options = {
        1: "select_searches_by_user",
        2: "select_user_navigation_sessions",
        3: "brands visited by user",
        4: "products views by user",
        5: "Purchases by user",
        6: "Clicks in ad's by user",
        7: "Time in category by user",
        8: "Clicks on notifications by user",
        9: "Errors per session by user",
        10: "Products promotions",
        11: "Exit"
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])

def usersNames():
    print("\n======Users available to see information======\n")
    print("Miguel Franco")
    print("Angel aceves")
    print("karen torres")
    print("omar madriz")
    print("abraham hernandez")
    print("juan pablo perez")
    print("emiliano villagran")
    print("================================================\n")

def promotionNames():
    keys = PROMOTIONS_IDS.keys()
    print("\n======Promotions available to see======\n")
    for i in keys:
        print(i)
    print("================================================\n")

def main():

    log.info("Connecting to Cluster")
    session = get_cassandra_session()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    model.bulk_insert(session)

    while True:
        print_menu()
        opt = int(input("Select an option: "))
        match opt:
            case 1:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.searches_by_user(session, user_uuid) 
            case 2:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.user_navigation_sessions(session,user_uuid)
            case 3:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.brands_searchs_by_user(session,user_uuid)
            case 4:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.products_views_by_user(session,user_uuid)
            case 5:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.purchases_by_user(session,user_uuid)
            case 6:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.clicked_ads_by_user(session,user_uuid)
            case 7:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.time_per_category_by_user(session,user_uuid)
            case 8:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.clicks_notifications_by_user(session,user_uuid)
            case 9:
                usersNames()
                user = input("\nType user name: ").strip().lower()
                if user not in USER_IDS:
                    print(f"User {user} not found")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model.errors_by_user(session,user_uuid)
            case 10:
                promotionNames()
                promotion = input("\nName of promotion: ")
                promotion_uuid = PROMOTIONS_IDS[promotion]
                print()
                model.promotions_by_user(session,promotion_uuid)
            case 11:
                break

        

if __name__ == '__main__':
    main()