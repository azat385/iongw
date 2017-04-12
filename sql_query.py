from sql_create_table import Base, engine, Data, db_name

import sqlite3

conn = sqlite3.connect(db_name)

dd = conn.execute("SELECT * FROM device;").fetchall()
tt = conn.execute("SELECT * FROM tag;").fetchall()
conn.close()

tag_name = "iongw1.m7.EactSumExport"
value = 330.3
stime = '2017-04-12 13:32:12.717345'
d, t = tag_name.split('.')[1:]

import numpy as np
dd = np.array(dd)
tt = np.array(tt)

def get_dev_id(name):
    return dd[dd[:,2]==name][0,0]

def get_tag_id(name):
    return tt[tt[:,2]==name][0,0]

d_id = get_dev_id(d)
t_id = get_tag_id(t)


Base.metadata.bind = engine

from sqlalchemy.orm import sessionmaker
DBSession = sessionmaker()
DBSession.bind = engine
session = DBSession()

# session.query(Data).all()

new_data = Data(device_id=get_dev_id(d),
                    tag_id=get_tag_id(t),
                    value=value,
                    stime=stime,
                    )
session.add(new_data)
session.commit()

