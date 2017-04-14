from sql_create_table import Data, Device, Tag
from sqlalchemy.ext.declarative import declarative_base
from yaml_data import server_name

table_prefix = server_name

Device.__tablename__ = "{}_{}".format(table_prefix, Device.__tablename__)
Tag.__tablename__ = "{}_{}".format(table_prefix, Tag.__tablename__)
Data.__tablename__ = "{}_{}".format(table_prefix, Data.__tablename__)

Base = declarative_base()

from sqlalchemy import create_engine, MetaData

url = 'postgresql://{}:{}@{}:{}/{}'
url = url.format('dbraw', ',fpflfyys[', '192.168.201.200', 5432, 'dbraw')
engine = create_engine(url, client_encoding='utf8', echo=True)

Base.metadata.create_all(engine)

meta = MetaData(bind=engine, reflect=True)

for table in meta.tables:
    print table
