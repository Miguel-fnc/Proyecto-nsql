#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId
from datetime import datetime

class OrderResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, oid):
        order = self.db.orders.find_one({"_id": ObjectId(oid)})
        if not order:
            resp.status = falcon.HTTP_404
            return

        # Convertir _id a string
        order["_id"] = str(order["_id"])

        # Convertir fechas
        for field in ["purchase_date"]:
            if field in order and isinstance(order[field], datetime):
                order[field] = order[field].isoformat()

        resp.media = order
        resp.status = falcon.HTTP_200

    async def on_delete(self, req, resp, oid):
        result = self.db.orders.delete_one({"_id": ObjectId(oid)})
        if result.deleted_count:
            resp.media = {"message": "Order deleted successfully"}
            resp.status = falcon.HTTP_200
        else:
            resp.media = {"error": "Order not found"}
            resp.status = falcon.HTTP_404


class OrdersResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        cursor = self.db.orders.find()
        orders = []

        for doc in cursor:
            # Convertir _id
            doc["_id"] = str(doc["_id"])

            # Convertir purchase_date
            if "purchase_date" in doc and isinstance(doc["purchase_date"], datetime):
                doc["purchase_date"] = doc["purchase_date"].isoformat()

            # Convertir shipping_address si contiene fechas en alguna implementación futura
            if "shipping_address" in doc:
                for k, v in doc["shipping_address"].items():
                    if isinstance(v, datetime):
                        doc["shipping_address"][k] = v.isoformat()

            orders.append(doc)

        resp.media = orders
        resp.status = falcon.HTTP_200

    async def on_post(self, req, resp):
        data = await req.media
        result = self.db.orders.insert_one(data)
        data["_id"] = str(result.inserted_id)
        resp.media = data
        resp.status = falcon.HTTP_201
