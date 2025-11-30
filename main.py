#!/usr/bin/env python3
import sys

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
            print("\n------ MODO MONGODB ------")
            print("MongoDB utiliza una API REST, no un menú interactivo.")
            print("Usa los siguientes comandos para consultar la base desde la terminal:\n")

            print("Comando\t\t\t\tDescripción")

            print("list_users\t\tLista todos los usuarios")
            print("get_user -i <id>\tMuestra un usuario por ID")
            print("list_orders\t\tLista todas las órdenes registradas")
            print("get_order -i <id>\tMuestra una orden específica")
            print("categories\t\tLista todas las categorías")
            print("brands\t\t\tLista todas las marcas")
            print("promotions\t\tLista las promociones registradas\n")

            print("Parámetro\t\t\tDescripción")

            print("--name\t\t\tBúsqueda por texto en nombre o descripción")
            print("--category\t\tFiltrar por categoría exacta")
            print("--min_price\t\tPrecio mínimo")
            print("--max_price\t\tPrecio máximo")
            print("--limit\t\t\tNúmero máximo de resultados")
            print("--skip\t\t\tNúmero de resultados a saltar (paginación)")


            print("\nEstructura general:")
            print("    python client.py <acción> [parámetros]\n")

            print("Ejemplos:")
            print("\tpython client.py list_products")
            print("\tpython client.py list_products --name correr")
            print("\tpython client.py list_products --category Playeras")
            print("\tpython client.py list_users")
            print("\tpython client.py get_user -i 6789123abcd...\n")

            print("Elige '4' para salir y usar la terminal para hacer requests desde la carpeta de Mongo.\n")

        elif db_option == 4:
            print("Saliendo...")
            sys.exit(0)
        else:
            print("Opción no válida")
            continue


if __name__ == "__main__":
    main_general()
