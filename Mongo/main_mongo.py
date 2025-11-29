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


# MongoDB Connection
client = MongoClient("mongodb://localhost:27017/")
db = client.ecommerce_sports   # <--- Base del proyecto del PDF

# Falcon
app = falcon.asgi.App(middleware=[LoggingMiddleware()])

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
