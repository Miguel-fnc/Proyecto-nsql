#!/usr/bin/env python3
import json
import pydgraph
import csv #Agregar csv

def set_schema(client):
    schema = """

    #crear nodos

    type Usuario {
        name
        email
        COMPRO
        VIO
        AGREGO_A_FAVORITOS
    }

    type Producto {
        name
        description
        precio
        SIMILAR_A
        PERTENECE_A
        EN_CATEGORIA
    }

    type Marca {
        name
        country
    }

    type Categoria {
        name
    }

    # indices

    name: string @index(term, fulltext, exact) .
    email: string .

    description: string .
    precio: float @index(float) .

    country: string .

    # relaciones

    COMPRO: [uid] @reverse .
    VIO: [uid] @reverse .
    AGREGO_A_FAVORITOS: [uid] @reverse .
    SIMILAR_A: [uid] @reverse .
    PERTENECE_A: uid @reverse .
    EN_CATEGORIA: uid @reverse .

    """
    return client.alter(pydgraph.Operation(schema=schema))

#funcion que crea el diccionario 
def csv_a_diccionario(file_path):
    resultado = {}
    i = 1

    with open(file_path, "r") as file:
        reader = csv.reader(file)
        #brincar los encabezados
        next(reader, None)
        
        for fila in reader:
            nombre = fila[1]
            resultado[i] = nombre
            i += 1
    return resultado

#funcion que imprime dicciconario

def imprimir_diccionario(diccionario):
    for clave, valor in diccionario.items():
        print(f"{clave}: {valor}")


#funciones para cargar datos mediante csv 

def load_usuarios(client,file_path):
    txn = client.txn()
    resp = None
    try:
        usuarios = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                usuarios.append({
                    'uid': row['uid'],
                    'dgraph.type': 'Usuario',
                    'name': row['name'],
                    'email': row['email']
                })
        resp = txn.mutate(set_obj=usuarios)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_productos(client,file_path):
    txn = client.txn()
    resp = None
    try:
        productos = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                productos.append({
                    'uid': row['uid'],
                    'dgraph.type': 'Producto',
                    'name': row['name'],
                    'description': row['description'],
                    'precio': float(row['precio'])
                })
        resp = txn.mutate(set_obj=productos)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_marcas(client, file_path):
    txn = client.txn()
    resp = None
    try:
        marcas = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                    marcas.append({
                        'uid': row['uid'],
                        'dgraph.type': 'Marca',
                        'name': row['marca'],
                        'country': row['country']
                    })
        resp = txn.mutate(set_obj=marcas)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids

def load_categorias(client, file_path):
    txn = client.txn()
    resp = None
    try:
        categorias = []
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)
            for row in reader:
                    categorias.append({
                        'uid': row['uid'],
                        'dgraph.type': 'Categoria',
                        'name': row['categoria']
                    })
        resp = txn.mutate(set_obj=categorias)
        txn.commit()
    finally:
        txn.discard()
    return resp.uids
def load_relaciones(client, file_path, uids_1, uids_2, predicate):
    txn = client.txn()
    try:
        with open(file_path, 'r') as file:
            reader = csv.DictReader(file)

            for row in reader:
                #quitar el prefijo _:
                left = row[list(row.keys())[0]].replace('_:', '')
                right = row[list(row.keys())[1]].replace('_:', '')

                mutation = {
                    'uid': uids_1[left],
                    predicate: {'uid': uids_2[right]}
                }
                txn.mutate(set_obj=mutation)

        txn.commit()
        
    except Exception:
        import traceback
        traceback.print_exc()
    finally:
        txn.discard()

# ------------ queries -----------------

# requerimiento 1
def productos_comprados(client, usuarios_dicc):
    print("\nPRODUCTOS COMPRADOS POR UN USUARIO")
    imprimir_diccionario(usuarios_dicc)
    seleccion = int(input("\nSelecciona el numero del usuario:\n> "))
    nombre = usuarios_dicc.get(seleccion)
    query = f"""
    {{
      compras(func: eq(name, "{nombre}")) {{
        name
        COMPRO {{
          name
          description
        }}
      }}
    }}
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

# requerimiento 2
def productos_mas_vistos(client):
    print("\nPRODUCTOS MAS VISTOS")
    query = """
    {
      var(func: type(Producto)) {
        vistas as count(~VIO)
      }
      
      mas_vistos(func: uid(vistas), orderdesc: val(vistas),first: 5) {
        name
        visualizaciones: val(vistas)
      }
    }
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

# requerimiento 3
def productos_por_categoria(client, categorias_dicc):
    print("\nPRODUCTOS POR CATEGORIA")
    imprimir_diccionario(categorias_dicc)
    seleccion = int(input("\nSelecciona el numero de la categoria:\n> "))
    categoria = categorias_dicc.get(seleccion)
    query = f"""
    {{
      por_categoria(func: eq(name, "{categoria}")) {{
        name
        ~EN_CATEGORIA {{
          name
        }}
      }}
    }}
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

# requerimiento 4
def mas_comprados(client):
    print("\nPRODUCTOS MAS COMPRADOS")

    query = """
    {
      var(func: type(Producto)) {
        compras as count(~COMPRO)
      }
      
      mas_comprados(func: uid(compras), orderdesc: val(compras), first: 5) {
        name
        total_compras: val(compras)
      }
    }
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

# requerimiento 5
def relaciones_cruzadas(client):
    print("\nANALISIS DE PREFERENCIA DE COMPRA POR USUARIO")

    query = """
    {
      user(func: type(Usuario)) {
        name
        COMPRO {
          name
          PERTENECE_A {
            name
          }
          EN_CATEGORIA {
            name
          }
        }
      }
    }
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

# requerimiento 6
def rango_precios(client):
    print("\nFILTRAR PRODUCTOS POR RANGO DE PRECIOS")
    minimo = input("Precio minimo:\n> ")
    maximo = input("Precio maximo:\n> ")

    query = f"""
    {{
      rango(func: type(Producto)) @filter(ge(precio, {minimo}) AND le(precio, {maximo})) {{
        name
        precio
      }}
    }}
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

#requerimiento 7
def productos_similares(client, productos_dicc):
    print("\nPRODUCTOS SIMILARES")
    imprimir_diccionario(productos_dicc)
    seleccion = int(input("\nSelecciona el numero del producto:\n> "))
    producto = productos_dicc.get(seleccion)

    query = f"""
    {{
      similares(func: eq(name, "{producto}")) {{
        name
        SIMILAR_A {{
          name
          PERTENECE_A {{ name }}
          EN_CATEGORIA {{ name }}
        }}
      }}
    }}
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

# requerimiento 8
def favoritos(client, usuarios_dicc):
    print("\nPRODUCTOS FAVORITOS DE UN USUARIO")
    imprimir_diccionario(usuarios_dicc)
    seleccion = int(input("\nSelecciona el numero del usuario:\n> "))
    nombre = usuarios_dicc.get(seleccion)
    query = f"""
    {{
      favoritos(func: eq(name, "{nombre}")) {{
        name
        AGREGO_A_FAVORITOS {{
          name
          precio
        }}
      }}
    }}
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

# requerimiento 9
def marcas_populares(client, categorias_dicc):
    print("\nMARCAS MAS POPULARES POR CATEGORIA")
    imprimir_diccionario(categorias_dicc)
    seleccion = int(input("\nSelecciona el numero de la categoria:\n> "))
    categoria = categorias_dicc.get(seleccion)

    query = f"""
    {{
      categorias(func: eq(name, "{categoria}")) {{
        name
        ~EN_CATEGORIA {{
          name
          PERTENECE_A {{
            name
            country
          }}
        }}
      }}
    }}
    """

    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        result = json.loads(res.json)
        
        # Procesar para contar ventas por marca
        if result.get('categorias'):
            marcas_ventas = {}
            for producto in result['categorias'][0].get('~EN_CATEGORIA', []):
                if 'PERTENECE_A' in producto:
                    marca = producto['PERTENECE_A']['name']
                    if marca not in marcas_ventas:
                        marcas_ventas[marca] = {
                            'name': marca,
                            'country': producto['PERTENECE_A'].get('country', ''),
                            'productos': 0
                        }
                    marcas_ventas[marca]['productos'] += 1
            
            # Ordenar por productos
            marcas_ordenadas = sorted(marcas_ventas.values(), 
                                     key=lambda x: x['productos'], 
                                     reverse=True)
            print(json.dumps(marcas_ordenadas, indent=2))
        else:
            print("No se encontró la categoría")
    finally:
        txn.discard()

# requerimeinto 10
def productos_vistos(client, usuarios_dicc):
    print("\nPRODUCTOS VISTOS POR UN USUARIO")
    imprimir_diccionario(usuarios_dicc)
    seleccion = int(input("\nSelecciona el numero del usuario:\n> "))
    nombre = usuarios_dicc.get(seleccion)
    
    query = f"""
    {{
      vistos(func: eq(name, "{nombre}")) {{
        name
        VIO {{
          name
        }}
      }}
    }}
    """
    txn = client.txn(read_only=True)
    try:
        res = txn.query(query)
        print(json.dumps(json.loads(res.json), indent=2))
    finally:
        txn.discard()

def drop_all(client):
    return client.alter(pydgraph.Operation(drop_all=True))