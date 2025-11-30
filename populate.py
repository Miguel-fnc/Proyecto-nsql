#POPULATE PARA CASSANDRA
#=======================================================================================
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cassandra.cluster import Cluster
from Cassandra.model import create_keyspace, create_schema, bulk_insert
from connect import get_cassandra_session
#=======================================================================================
import csv
# Importar funciones de Dgraph
from Dgraph.model_dgraph import (
    set_schema, csv_a_diccionario, load_marcas, load_categorias, 
    load_productos, load_usuarios, load_relaciones,
    productos_comprados, productos_mas_vistos, productos_por_categoria,
    mas_comprados, relaciones_cruzadas, rango_precios, productos_similares,
    favoritos, marcas_populares, productos_vistos, drop_all
)
from connect import get_dgraph_client
#=======================================================================================
# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'ecommerce')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

log.info("Connecting to Cluster")
session = get_cassandra_session()

create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
session.set_keyspace(KEYSPACE)
create_schema(session)
bulk_insert(session)
#=======================================================================================

# Inicializar Dgraph
log.info("Connecting to Dgraph")
dgraph_client = get_dgraph_client()
set_schema(dgraph_client)

def create_data(client):
#primero cargamos los noods
    marcas_uids = load_marcas(client, '../data/Dgraph/marcas.csv')
    categorias_uids = load_categorias(client, '../data/Dgraph/categorias.csv')
    productos_uids = load_productos(client, '../data/Dgraph/productos.csv')
    usuarios_uids = load_usuarios(client, '../data/Dgraph/usuarios.csv')

#despues cargar relaciones
    load_relaciones(client, "../data/Dgraph/relacion_pertenece.csv", productos_uids, marcas_uids, "PERTENECE_A")
    load_relaciones(client, "../data/Dgraph/relacion_en_categoria.csv", productos_uids, categorias_uids, "EN_CATEGORIA")
    load_relaciones(client, "../data/Dgraph/relacion_compro.csv", usuarios_uids, productos_uids, "COMPRO")
    load_relaciones(client, "../data/Dgraph/relacion_vio.csv", usuarios_uids, productos_uids, "VIO")
    load_relaciones(client, "../data/Dgraph/relacion_favoritos.csv", usuarios_uids, productos_uids, "AGREGO_A_FAVORITOS")

    #generar diccionarios para simplificar el menu
    marcas_dicc = csv_a_diccionario('../data/Dgraph/marcas.csv')
    categoria_dicc = csv_a_diccionario('../data/Dgraph/categorias.csv')
    productos_dicc = csv_a_diccionario('../data/Dgraph/productos.csv')
    usuarios_dicc = csv_a_diccionario('../data/Dgraph/usuarios.csv')

     #retornar los diccionarios
    return {
        'usuarios': usuarios_dicc,
        'productos': productos_dicc,
        'marcas': marcas_dicc,
        'categorias': categoria_dicc
    }

# Exponer las funciones de consulta para que main.py las use
def consulta_productos_comprados(usuarios_dicc):
    productos_comprados(dgraph_client, usuarios_dicc)

def consulta_productos_mas_vistos():
    productos_mas_vistos(dgraph_client)

def consulta_productos_por_categoria(categorias_dicc):
    productos_por_categoria(dgraph_client, categorias_dicc)

def consulta_mas_comprados():
    mas_comprados(dgraph_client)

def consulta_relaciones_cruzadas():
    relaciones_cruzadas(dgraph_client)

def consulta_rango_precios():
    rango_precios(dgraph_client)

def consulta_productos_similares(productos_dicc):
    productos_similares(dgraph_client, productos_dicc)

def consulta_favoritos(usuarios_dicc):
    favoritos(dgraph_client, usuarios_dicc)

def consulta_marcas_populares(categorias_dicc):
    marcas_populares(dgraph_client, categorias_dicc)

def consulta_productos_vistos(usuarios_dicc):
    productos_vistos(dgraph_client, usuarios_dicc)

def borrar_todo():
    drop_all(dgraph_client)