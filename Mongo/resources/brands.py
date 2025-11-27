#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId

class BrandsResource:
    def __init__(self, db):
        self.db = db

