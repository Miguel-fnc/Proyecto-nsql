import os
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pymongo import MongoClient
import pydgraph


# ---------- MONGODB ----------
def get_mongo_client():
    mongo_uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    client = MongoClient(mongo_uri)
    return client

# ---------- CASSANDRA ----------
def get_cassandra_session():
    CLUSTER_IPS = os.getenv("CASSANDRA_CLUSTER_IPS", "127.0.0.1").split(",")

    cluster = Cluster(CLUSTER_IPS)
    session = cluster.connect()

    return session

# ---------- DGRAPH ----------
def get_dgraph_client():
    addr = os.getenv("DGRAPH_ADDR", "localhost:9080")
    stub = pydgraph.DgraphClientStub(addr)
    client = pydgraph.DgraphClient(stub)
    return client

    
