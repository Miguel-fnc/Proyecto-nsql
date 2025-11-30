#!/usr/bin/env python3
import sys
import os
import populate

def main_general():
    print("A que base de datos quieres acceder?")
    print("1. Cassandra\n2. Dgraph\n3. MongoDB")
    db_option = int(input("Selecciona una opción: "))
    match db_option:
        case 1:
            from cass import app as cass_app
            cass_app.main()
        case 2:
            pass
            #from Dgraph import main as dgraph_app
            #dgraph_app.main()
        case 3:
            pass
            #from mongo import app as mongo_app
            #mongo_app.main()
        case _:
            pass
            #print("Opción no válida")
            #sys.exit(1)

main_general()
"""def print_menu():
    mm_options = {
        1: "Productos comprados por un usuario",
        2: "Top 5 Productos mas vistos",
        3: "Productos por categoria",
        4: "Top 5 Productos más comprados",
        5: "Analisis de preferencias de compra por usuario",
        6: "Filtrar productos por rango de precios",
        7: "Productos similares",
        8: "Productos favoritos de un usuario",
        9: "Marcas mas populares por categoria",
        10: "Productos vistos por un usuario",
        11: "Borrar TODO",
        12: "Salir"
    }
    print("\n------ MENU DE OPCIONES ------")
    for key in mm_options.keys():
        print(key, '--', mm_options[key])
    print("---------------------------\n")


def main():
    diccionarios = populate.diccionarios

    while True:
        print_menu()
        option = int(input('Enter your choice: '))
        
        if option == 1:
            populate.consulta_productos_comprados(diccionarios['usuarios'])

        elif option == 2:
            populate.consulta_productos_mas_vistos()

        elif option == 3:
            populate.consulta_productos_por_categoria(diccionarios['categorias'])

        elif option == 4:
            populate.consulta_mas_comprados()

        elif option == 5:
            populate.consulta_relaciones_cruzadas()

        elif option == 6:
            populate.consulta_rango_precios()

        elif option == 7:
            populate.consulta_productos_similares(diccionarios['productos'])

        elif option == 8:
            populate.consulta_favoritos(diccionarios['usuarios'])

        elif option == 9:
            populate.consulta_marcas_populares(diccionarios['categorias'])

        elif option == 10:
            populate.consulta_productos_vistos(diccionarios['usuarios'])

        elif option == 11:
            confirm = input("\n¿Seguro que deseas borrar TODO? (s/n): ")
            if confirm.lower() == "s":
                populate.borrar_todo()
                print("\nBase de datos eliminada\n")
                # Recargar diccionarios vacíos
                diccionarios = {
                    'usuarios': {},
                    'productos': {},
                    'marcas': {},
                    'categorias': {}
                }

        elif option == 12:
            print("\nSaliendo...\n")
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))"""