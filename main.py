#!/usr/bin/env python3
import sys
import populate

def main_general():
    
    while True:
        print("A qué base de datos quieres acceder?")
        print("1. Cassandra\n2. Dgraph\n3. MongoDB\n4. Salir")
        db_option = int(input("Selecciona una opción: "))

        if db_option == 1:
            print("\n")
            from cass.app import main as cass_main
            cass_main()

        elif db_option == 2:
            from Dgraph.main import main as dgraph_main
            dgraph_main()

        elif db_option == 3:
            from Mongo.mongo_menu import mongo_menu
            mongo_menu()

        elif db_option == 4:
            print("Saliendo...")
            populate.borrar_todo()
            sys.exit(0)
            
        else:
            print("Opción no válida")
            continue


if __name__ == "__main__":
    main_general()
