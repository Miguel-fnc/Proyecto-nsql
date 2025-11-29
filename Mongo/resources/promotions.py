#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId
from datetime import datetime

class PromotionsResource:
    def __init__(self, db):
        self.db = db

    async def on_get(self, req, resp):
        cursor = self.db.promotions.find()
        out = []

        for doc in cursor:
            doc["_id"] = str(doc["_id"])

            # Convertir fechas a str
            for field in ["start_date", "end_date"]:
                if field in doc and isinstance(doc[field], datetime):
                    doc[field] = doc[field].isoformat()

            out.append(doc)

        resp.media = out
        resp.status = falcon.HTTP_200
