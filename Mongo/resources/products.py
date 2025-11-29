#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId

class ProductResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, pid):
        product = self.db.products.find_one({'_id': ObjectId(pid)})
        if product:
            product['_id'] = str(product['_id'])
            resp.media = product
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404


class ProductsResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        name = req.get_param('name')
        category = req.get_param('category')
        min_price = req.get_param_as_float('min_price')
        max_price = req.get_param_as_float('max_price')
        limit = req.get_param_as_int('limit') or 20
        skip = req.get_param_as_int('skip') or 0

        query = {}

        if name:
            query["$text"] = { "$search": f"\"{name}\"" }

        if category:
            query["category_id"] = category

        if min_price is not None:
            query["price"] = { "$gte": min_price }

        if max_price is not None:
            query.setdefault("price", {})
            query["price"]["$lte"] = max_price

        cursor = self.db.products.find(query).skip(skip).limit(limit)
        results = []

        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            results.append(doc)

        resp.media = results
        resp.status = falcon.HTTP_200
