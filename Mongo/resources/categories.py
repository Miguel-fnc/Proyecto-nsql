#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId

class CategoriesResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        cursor = self.db.categories.find()
        items = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            items.append(doc)
        resp.media = items

    async def on_post(self, req, resp):
        data = await req.media
        result = self.db.categories.insert_one(data)
        data["_id"] = str(result.inserted_id)
        resp.media = data
        resp.status = falcon.HTTP_201

