#!/usr/bin/env python3
import logging
import os
import uuid
from connect import get_cassandra_session
from cass.data.data_cassandra import USER_IDS, PROMOTIONS_IDS
from cass import model_cassandra

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)



def print_menu():
    mm_options = {
        1: "Seleccionar búsquedas por usuario",
        2: "Seleccionar sesiones de navegación del usuario",
        3: "Marcas visitadas por el usuario",
        4: "Vistas de productos por el usuario",
        5: "Compras por el usuario",
        6: "Clics en anuncios por el usuario",
        7: "Tiempo en categoría por el usuario",
        8: "Clics en notificaciones por el usuario",
        9: "Errores por sesión del usuario",
        10: "Promociones de productos",
        11: "Salir"
    }
    for key in mm_options.keys():
        print(key, '--', mm_options[key])


def usersNames():
    print("\n======Usuarios disponibles para ver información======\n")
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
    print("\n======Promociones disponibles para ver======\n")
    for i in keys:
        print(i)
    print("================================================\n")

def main():
    KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'ecommerce')

    log.info("Connecting to Cluster")
    session = get_cassandra_session()
    session.set_keyspace(KEYSPACE)

    while True:
        print_menu()
        opt = int(input("\nSeleccione una opcion: "))
        match opt:
            case 1:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.searches_by_user(session, user_uuid) 
            case 2:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.user_navigation_sessions(session,user_uuid)
            case 3:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.brands_searchs_by_user(session,user_uuid)
            case 4:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.products_views_by_user(session,user_uuid)
            case 5:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.purchases_by_user(session,user_uuid)
            case 6:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.clicked_ads_by_user(session,user_uuid)
            case 7:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.time_per_category_by_user(session,user_uuid)
            case 8:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.clicks_notifications_by_user(session,user_uuid)
            case 9:
                usersNames()
                user = input("\nEscriba el nombre del usuario: ").strip().lower()
                if user not in USER_IDS:
                    print(f"Usuario {user} no encontrado")
                    continue
                user_uuid = USER_IDS[user]
                print()
                model_cassandra.errors_by_user(session,user_uuid)
            case 10:
                promotionNames()
                promotion = input("\nEscriba el nombre de la promoción: ").strip().lower()
                if promotion not in PROMOTIONS_IDS:
                    print(f"Promoción {promotion} no encontrada")
                    continue
                promotion_uuid = PROMOTIONS_IDS[promotion]
                print()
                model_cassandra.promotions_by_user(session,promotion_uuid)
            case 11:
                break

        

if __name__ == '__main__':
    main()