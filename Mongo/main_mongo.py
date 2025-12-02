#!/usr/bin/env python3
import falcon.asgi
from pymongo import MongoClient
import logging

from resources.products import ProductResource, ProductsResource
from resources.users import UserResource, UsersResource
from resources.orders import OrderResource, OrdersResource
from resources.categories import CategoriesResource
from resources.brands import BrandsResource
from resources.promotions import PromotionsResource

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class LoggingMiddleware:
    async def process_request(self, req, resp):
        logger.info(f"Request: {req.method} {req.uri}")

    async def process_response(self, req, resp, resource, req_succeeded):
        logger.info(f"Response: {resp.status} for {req.method} {req.uri}")

#  Creación de índices en MongoDB

def ensure_indexes(db):
    print("\n→ Verificando índices de MongoDB...")

    # Products
    db.products.create_index({
        "name": "text",
        "description": "text"
    })
    db.products.create_index([("category_id", 1), ("price", 1)])
    db.products.create_index([("inventory.size", 1)])

    # Orders
    db.orders.create_index([("user_id", 1), ("purchase_date", -1)])

    # Categories
    db.categories.create_index([("name", "text")])

    # Brands
    db.brands.create_index([("name", "text")])

    # Promotions
    db.promotions.create_index([("start_date", 1), ("end_date", 1)])

# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client.ecommerce_sports   
ensure_indexes(db)

# Falcon
app = falcon.asgi.App(middleware=[LoggingMiddleware()])

class DropMongoResource:
    def __init__(self, db):
        self.db = db

    async def on_delete(self, req, resp):
        collections = self.db.list_collection_names()
        for col in collections:
            self.db[col].drop()

        resp.media = {
            "status": "ok",
            "dropped": collections
        }

# Rutas
app.add_route("/products", ProductsResource(db))
app.add_route("/products/{pid}", ProductResource(db))

app.add_route("/users", UsersResource(db))
app.add_route("/users/{uid}", UserResource(db))

app.add_route("/orders", OrdersResource(db))
app.add_route("/orders/{oid}", OrderResource(db))

app.add_route("/categories", CategoriesResource(db))
app.add_route("/brands", BrandsResource(db))
app.add_route("/promotions", PromotionsResource(db))

app.add_route("/drop_mongo", DropMongoResource(db))
