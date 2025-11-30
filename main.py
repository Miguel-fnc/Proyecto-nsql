#!/usr/bin/env python3
import sys
import os
import populate

def print_menu():
    mm_options = {
        1: "Cargar Datos",
        2: "Productos comprados por un usuario",
        3: "Top 5 Productos mas vistos",
        4: "Productos por categoria",
        5: "Top 5 Productos más comprados",
        6: "Analisis de preferencias de compra por usuario",
        7: "Filtrar productos por rango de precios",
        8: "Productos similares",
        9: "Productos favoritos de un usuario",
        10: "Marcas mas populares por categoria",
        11: "Productos vistos por un usuario",
        12: "Borrar TODO",
        13: "Salir"
    }
    print("\n------ MENU DE OPCIONES ------")
    for key in mm_options.keys():
        print(key, '--', mm_options[key])
    print("---------------------------\n")


def main():
    # Inicializar diccionarios vacíos
    diccionarios = {
        'usuarios': {},
        'productos': {},
        'marcas': {},
        'categorias': {}
    }

    while True:
        print_menu()
        option = int(input('Enter your choice: '))
        
        if option == 1:
            diccionarios = populate.create_data()

        elif option == 2:
            populate.consulta_productos_comprados(diccionarios['usuarios'])

        elif option == 3:
            populate.consulta_productos_mas_vistos()

        elif option == 4:
            populate.consulta_productos_por_categoria(diccionarios['categorias'])

        elif option == 5:
            populate.consulta_mas_comprados()

        elif option == 6:
            populate.consulta_relaciones_cruzadas()

        elif option == 7:
            populate.consulta_rango_precios()

        elif option == 8:
            populate.consulta_productos_similares(diccionarios['productos'])

        elif option == 9:
            populate.consulta_favoritos(diccionarios['usuarios'])

        elif option == 10:
            populate.consulta_marcas_populares(diccionarios['categorias'])

        elif option == 11:
            populate.consulta_productos_vistos(diccionarios['usuarios'])

        elif option == 12:
            confirm = input("\n¿Seguro que deseas borrar TODO? (s/n): ")
            if confirm.lower() == "s":
                populate.borrar_todo()
                print("\nBase de datos eliminada\n")
                # Vaciar diccionarios
                diccionarios = {
                    'usuarios': {},
                    'productos': {},
                    'marcas': {},
                    'categorias': {}
                }

        elif option == 13:
            print("Saliendo...")
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))