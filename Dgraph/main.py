import populate

def print_menu():
    mm_options = {
        1: "Productos comprados por un usuario",
        2: "Top 5 Productos más vistos",
        3: "Productos por categoría",
        4: "Top 5 Productos más comprados",
        5: "Análisis de preferencias de compra por usuario",
        6: "Filtrar productos por rango de precios",
        7: "Productos similares",
        8: "Productos favoritos de un usuario",
        9: "Marcas más populares por categoría",
        10: "Productos vistos por un usuario",
        11: "Borrar TODO",
        12: "Salir"
    }

    print("\n------ MENU DGRAPH ------")
    for key in mm_options.keys():
        print(key, '--', mm_options[key])
    print("---------------------------\n")


def main():
    diccionarios = populate.diccionarios

    while True:
        print_menu()
        option = int(input("Selecciona una opción: "))
    
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
            confirm = input("¿Seguro que deseas borrar TODO? (s/n): ")
            if confirm.lower() == "s":
                populate.borrar_todo()
                print("Base de datos eliminada")
                diccionarios = { 'usuarios': {}, 'productos': {}, 'marcas': {}, 'categorias': {} }

        elif option == 12:
            print("Saliendo de Dgraph...")
            return  # Esto regresa al main general

        else:
            print("Opción inválida")
