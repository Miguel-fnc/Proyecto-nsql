#POPULATE PARA CASSANDRA
#=======================================================================================
import logging
import sys
import os
import requests
from cass.model_cassandra import create_keyspace, create_schema, bulk_insert
from connect import get_cassandra_session, get_dgraph_client
# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'ecommerce')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

# Inicializar Cassandra
log.info("Connecting to Cassandra Cluster")
cassandra_session = get_cassandra_session()
create_keyspace(cassandra_session, KEYSPACE, REPLICATION_FACTOR)
cassandra_session.set_keyspace(KEYSPACE)
create_schema(cassandra_session)
bulk_insert(cassandra_session)

# FUNCIONES CASSANDRA (importar desde model_cassandra)
from cass.model_cassandra import (
    searches_by_user, user_navigation_sessions, brands_searchs_by_user,
    products_views_by_user, purchases_by_user, clicked_ads_by_user,
    time_per_category_by_user, clicks_notifications_by_user, errors_by_user,
    promotions_by_user
)
#=======================================================================================ß
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

def consulta_searches_by_user(user_id):
    searches_by_user(cassandra_session, user_id)

def consulta_user_navigation_sessions(user_id):
    user_navigation_sessions(cassandra_session, user_id)

def consulta_brands_by_user(user_id):
    brands_searchs_by_user(cassandra_session, user_id)

def consulta_products_views_by_user(user_id):
    products_views_by_user(cassandra_session, user_id)

def consulta_purchases_by_user(user_id):
    purchases_by_user(cassandra_session, user_id)

def consulta_ad_clicks_by_user(user_id):
    clicked_ads_by_user(cassandra_session, user_id)

def consulta_time_per_category(user_id):
    time_per_category_by_user(cassandra_session, user_id)

def consulta_notifications_clicks(user_id):
    clicks_notifications_by_user(cassandra_session, user_id)

def consulta_errors_by_user(user_id):
    errors_by_user(cassandra_session, user_id)

def consulta_promotions(promotion_id):
    promotions_by_user(cassandra_session, promotion_id)

def borrar_todo():
    # Borrar Dgraph
    log.info("Borrando datos de Dgraph...")
    drop_all(dgraph_client)
    # Borrar Cassandra
    log.info("Borrando datos de Cassandra...")
    try:
        from connect import get_cassandra_session
        from cass import model_cassandra
        import os
        
        KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'ecommerce')
        session = get_cassandra_session()
        session.set_keyspace(KEYSPACE)
        
        model_cassandra.borrar_cassandra(session)
        session.shutdown()
    except Exception as e:
        print(f"Error al borrar Cassandra: {e}")
    
    
#=======================================================================================

# Inicializar Dgraph
log.info("Connecting to Dgraph")
dgraph_client = get_dgraph_client()
set_schema(dgraph_client)

log.info("Loading Dgraph data...")

#primero cargamos los noods
marcas_uids = load_marcas(dgraph_client, './data/Dgraph/marcas.csv')
categorias_uids = load_categorias(dgraph_client, './data/Dgraph/categorias.csv')
productos_uids = load_productos(dgraph_client, './data/Dgraph/productos.csv')
usuarios_uids = load_usuarios(dgraph_client, './data/Dgraph/usuarios.csv')

#despues cargar relaciones
load_relaciones(dgraph_client, "./data/Dgraph/relacion_pertenece.csv", productos_uids, marcas_uids, "PERTENECE_A")
load_relaciones(dgraph_client, "./data/Dgraph/relacion_en_categoria.csv", productos_uids, categorias_uids, "EN_CATEGORIA")
load_relaciones(dgraph_client, "./data/Dgraph/relacion_compro.csv", usuarios_uids, productos_uids, "COMPRO")
load_relaciones(dgraph_client, "./data/Dgraph/relacion_vio.csv", usuarios_uids, productos_uids, "VIO")
load_relaciones(dgraph_client, "./data/Dgraph/relacion_favoritos.csv", usuarios_uids, productos_uids, "AGREGO_A_FAVORITOS")
load_relaciones(dgraph_client, "./data/Dgraph/relacion_similar.csv", productos_uids, productos_uids, "SIMILAR_A")

#generar diccionarios para simplificar el menu
marcas_dicc = csv_a_diccionario('./data/Dgraph/marcas.csv')
categoria_dicc = csv_a_diccionario('./data/Dgraph/categorias.csv')
productos_dicc = csv_a_diccionario('./data/Dgraph/productos.csv')
usuarios_dicc = csv_a_diccionario('./data/Dgraph/usuarios.csv')

diccionarios = {
    'usuarios': csv_a_diccionario('./data/Dgraph/usuarios.csv'),
    'productos': csv_a_diccionario('./data/Dgraph/productos.csv'),
    'marcas': csv_a_diccionario('./data/Dgraph/marcas.csv'),
    'categorias': csv_a_diccionario('./data/Dgraph/categorias.csv')
}

# funciones de consulta para que main ßlas use
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


#=======================================================================================
# POPULATE MONGO

MONGO_BASE_URL = "http://localhost:8000"


def mongo_post_many(endpoint, records):
    url = f"{MONGO_BASE_URL}/{endpoint}"
    log.info(f"Populating Mongo endpoint: {endpoint}")

    for rec in records:
        r = requests.post(url, json=rec)

        if not r.ok:
            log.error(f"Error posting to {endpoint}: {r.status_code} - {r.text}")
        else:
            log.info(f"Inserted into {endpoint}: {rec.get('name', rec.get('title', 'item'))}")


def load_csv(path):
    try:
        with open(path, encoding="utf-8") as fd:
            return list(csv.DictReader(fd))
    except FileNotFoundError:
        log.warning(f"CSV not found: {path}")
        return []


def populate_mongo():
    log.info("=== Populating MongoDB ===")

    # 1. Categories
    categories = load_csv(".data/Mongo/categories.csv")
    mongo_post_many("categories", categories)

    # 2. Brands
    brands = load_csv(".data/Mongo/brands.csv")
    mongo_post_many("brands", brands)

    # 3. Products
    products = load_csv(".data/Mongo/products.csv")
    for p in products:
        if "price" in p:
            p["price"] = float(p["price"])
    mongo_post_many("products", products)

    # 4. Users
    users = load_csv(".data/Mongo/users.csv")
    mongo_post_many("users", users)

    # 5. Orders
    orders = load_csv(".data/Mongo/orders.csv")
    for o in orders:
        if "total" in o:
            o["total"] = float(o["total"])
    mongo_post_many("orders", orders)

    # 6. Promotions
    promotions = load_csv(".data/Mongo/promotions.csv")
    for promo in promotions:
        if "discount" in promo:
            promo["discount"] = float(promo["discount"])
    mongo_post_many("promotions", promotions)

    log.info("=== MongoDB Population Completed ===")
