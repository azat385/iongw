# from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sql_create_table import Base, Device, Tag

from yaml_data import whole_tag, id_data_list

from sqlalchemy import create_engine, MetaData

url = 'postgresql://{}:{}@{}:{}/{}'
url = url.format('dbraw', ',fpflfyys[', '192.168.201.200', 5432, 'dbraw')
engine = create_engine(url, client_encoding='utf8', echo=True)

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

session = DBSession()

tag_list = whole_tag

for tag in tag_list:
    new_tag = Tag(order_num=tag[0],
                  name=tag[1],
                  )
    session.add(new_tag)
session.commit()



dev_list = id_data_list
# Insert a Person in the person table
for dev in dev_list:
    new_dev = Device(slave_num=dev[0],
                     name=dev[1],
                     desc_short=dev[2],
                     #desc_long=dev[3],
                     )
    session.add(new_dev)
session.commit()


# check results
session.query(Device).all()

# Insert an Address in the address table
# new_address = Address(post_code='2222', person=new_person)
# session.add(new_address)
# session.commit()