#!/usr/bin/env python3
import falcon
from bson.objectid import ObjectId

class UserResource:
    def __init__(self, db):
        self.db = db

