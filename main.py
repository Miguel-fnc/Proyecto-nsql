#!/usr/bin/env python3
import sys

def main_general():
    print("A qué base de datos quieres acceder?")
    print("1. Cassandra\n2. Dgraph\n3. MongoDB")
    db_option = int(input("Selecciona una opción: "))

    if db_option == 1:
        from cass.app import main as cass_main
        cass_main()

    elif db_option == 2:
        from Dgraph.main import main as dgraph_main
        dgraph_main()

    #elif db_option == 3:
    #    from mongo.app import main as mongo_main
    #    mongo_main()

    else:
        print("Opción no válida")
        sys.exit(1)


if __name__ == "__main__":
    main_general()
