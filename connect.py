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
    KEYSPACE = os.getenv("CASSANDRA_KEYSPACE", "ecommerce")
    RF = os.getenv("CASSANDRA_REPLICATION_FACTOR", "1")

    cluster = Cluster(CLUSTER_IPS)
    session = cluster.connect()

    # Crear keyspace (igual que en tu app.py)
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {KEYSPACE}
        WITH replication = {{
            'class': 'SimpleStrategy',
            'replication_factor': '{RF}'
        }};
    """)

    session.set_keyspace(KEYSPACE)
    return session

# ---------- DGRAPH ----------
def get_dgraph_client():
    addr = os.getenv("DGRAPH_ADDR", "localhost:9080")
    stub = pydgraph.DgraphClientStub(addr)
    client = pydgraph.DgraphClient(stub)
    return client

    
