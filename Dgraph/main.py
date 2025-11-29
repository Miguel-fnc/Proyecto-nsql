#!/usr/bin/env python3
import os
import pydgraph
import model

DGRAPH_URI = os.getenv('DGRAPH_URI', 'localhost:9080')

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


# Conexión directa con el servidor Dgraph usando gRPC.
def create_client_stub():
    return pydgraph.DgraphClientStub(DGRAPH_URI)

# Crea el cliente principal de Dgraph: realizar operaciones de alto nivel como queries, mutaciones y transacciones.
def create_client(client_stub):
    return pydgraph.DgraphClient(client_stub)

def close_client_stub(client_stub):
    client_stub.close()

def main():
    # Inicializar Client Stub y Dgraph Client
    client_stub = create_client_stub()
    client = create_client(client_stub)

    # Crear schema
    model.set_schema(client)

    # Inicializar diccionarios vacíos
    diccionarios = {
        'usuarios': {},
        'productos': {},
        'marcas': {},
        'categorias': {}
    }

    while(True):
        print_menu()
        option = int(input('Enter your choice: '))
        
        if option == 1:
            diccionarios = model.create_data(client)

        if option == 2:
            model.productos_comprados(client, diccionarios['usuarios'])

        if option == 3:
            model.productos_mas_vistos(client)

        if option == 4:
            model.productos_por_categoria(client, diccionarios['categorias'])

        if option == 5:
            model.mas_comprados(client)

        if option == 6:
            model.relaciones_cruzadas(client)

        if option == 7:
            model.rango_precios(client)

        if option == 8:
            model.productos_similares(client, diccionarios['productos'])

        if option == 9:
            model.favoritos(client, diccionarios['usuarios'])

        if option == 10:
            model.marcas_populares(client, diccionarios['categorias'])

        if option == 11:
            model.productos_vistos(client, diccionarios['usuarios'])

        if option == 12:
            confirm = input("\n¿Seguro que deseas borrar TODO? (s/n): ")
            if confirm.lower() == "s":
                model.drop_all(client)
                print("\nBase de datos eliminada\n")
            #vaciar diccionarios
                diccionarios = {
                    'usuarios': {},
                    'productos': {},
                    'marcas': {},
                    'categorias': {}
                }

        if option == 13:
            close_client_stub(client_stub)
            exit(0)


if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        print('Error: {}'.format(e))