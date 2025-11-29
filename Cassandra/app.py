#!/usr/bin/env python3
import logging
import os
import uuid
from cassandra.cluster import Cluster
import model

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
CLUSTER_IPS = os.getenv('CASSANDRA_CLUSTER_IPS', '127.0.0.1')
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



def main():

    log.info("Connecting to Cluster")
    cluster = Cluster(CLUSTER_IPS.split(','))
    session = cluster.connect()

    model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
    session.set_keyspace(KEYSPACE)

    model.create_schema(session)

    model.bulk_insert(session)

    while True:
        print_menu()
        opt = int(input("Select an option: "))
        match opt:
            case 1:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.searches_by_user(session, user_uuid) 
            case 2:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.user_navigation_sessions(session,user_uuid)
            case 3:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.brands_searchs_by_user(session,user_uuid)
            case 4:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.products_views_by_user(session,user_uuid)
            case 5:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.purchases_by_user(session,user_uuid)
            case 6:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.clicked_ads_by_user(session,user_uuid)
            case 7:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.time_per_category_by_user(session,user_uuid)
            case 8:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.clicks_notifications_by_user(session,user_uuid)
            case 9:
                user = input("\nType user id: ")
                user_uuid = uuid.UUID(user)
                print()
                model.errors_by_user(session,user_uuid)
            case 10:
                promotion = input("\nType id of promotion to see: ")
                promotion_uuid = uuid.UUID(promotion)
                print()
                model.promotions_by_user(session,promotion_uuid)
            case 11:
                break

        

if __name__ == '__main__':
    main()