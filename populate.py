#POPULATE PARA CASSANDRA
# Sample data
import logging
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from cassandra.cluster import Cluster
import model
from connect import get_cassandra_session

# Set logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('logistics.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

# Read env vars related to Cassandra App
KEYSPACE = os.getenv('CASSANDRA_KEYSPACE', 'ecommerce')
REPLICATION_FACTOR = os.getenv('CASSANDRA_REPLICATION_FACTOR', '1')

log.info("Connecting to Cluster")
session = get_cassandra_session()

model.create_keyspace(session, KEYSPACE, REPLICATION_FACTOR)
session.set_keyspace(KEYSPACE)
model.create_schema(session)
model.bulk_insert(session)