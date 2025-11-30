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

            # Convertir fechas a string ISO
            for field in ["start_date", "end_date"]:
                if field in doc and isinstance(doc[field], datetime):
                    doc[field] = doc[field].isoformat()

            out.append(doc)

        resp.media = out
        resp.status = falcon.HTTP_200

    async def on_post(self, req, resp):
        data = await req.media

        # Manejo simple de discount -> float
        if "discount" in data:
            try:
                data["discount"] = float(data["discount"])
            except:
                pass

        # Fechas opcionales
        for f in ["start_date", "end_date"]:
            if f in data and data[f] != "":
                try:
                    data[f] = datetime.fromisoformat(data[f])
                except:
                    pass

        result = self.db.promotions.insert_one(data)
        data["_id"] = str(result.inserted_id)

        resp.media = data
        resp.status = falcon.HTTP_201
