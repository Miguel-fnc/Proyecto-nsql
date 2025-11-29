#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId

class UserResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp, uid):
        user = self.db.users.find_one({'_id': ObjectId(uid)})
        if user:
            user['_id'] = str(user['_id'])
            resp.media = user
            resp.status = falcon.HTTP_200
        else:
            resp.status = falcon.HTTP_404

    async def on_delete(self, req, resp, uid):
        result = self.db.users.delete_one({'_id': ObjectId(uid)})
        if result.deleted_count:
            resp.media = {"message": "User deleted successfully"}
            resp.status = falcon.HTTP_200
        else:
            resp.media = {"error": "User not found"}
            resp.status = falcon.HTTP_404


class UsersResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        cursor = self.db.users.find()
        users = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            users.append(doc)

        resp.media = users
        resp.status = falcon.HTTP_200

    async def on_post(self, req, resp):
        data = await req.media
        data.setdefault("addresses", [])
        data.setdefault("recent_purchases", [])
        data.setdefault("cart", [])
        data.setdefault("favorites", [])

        result = self.db.users.insert_one(data)
        data["_id"] = str(result.inserted_id)
        resp.media = data
        resp.status = falcon.HTTP_201
