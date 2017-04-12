# from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sql_create_table import Base, engine, Device, Tag, Data

import yaml_data

Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

session = DBSession()

tag_list = yaml_data.whole_tag

for tag in tag_list:
    new_tag = Tag(order_num=tag[0],
                  name=tag[1],
                  )
    session.add(new_tag)
session.commit()



dev_list = yaml_data.id_data_list
# Insert a Person in the person table
for dev in dev_list:
    new_dev = Device(slave_num=dev[0],
                     name=dev[1],
                     desc_short=dev[2],
                     #desc_long=dev[3],
                     )
    session.add(new_dev)
session.commit()


# Insert an Address in the address table
# new_address = Address(post_code='2222', person=new_person)
# session.add(new_address)
# session.commit()