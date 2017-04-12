#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging
from logging.handlers import RotatingFileHandler

# format the log entries
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

handler = RotatingFileHandler('mc2db.log',
                              mode='a',
                              maxBytes=20*1024*1024,
                              backupCount=5,
                              encoding=None,
                              delay=0)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from sql_create_table import Base, engine, db_name, Data

import sqlite3
import memcache
mc = memcache.Client(['127.0.0.1:11211'], debug=0)
#mc.flush_all()


def get_data_array(val):
    try:
        valT = val.split(";;;")
        arr = []
        for val in valT:
            arr.append(val.split(";"))
        return arr
    except:
        return None


def write_data_to_db(_key, arr):
    #import sqlite3
    conn = sqlite3.connect(db_name)
    #print "Opened database successfully";
    if check_table_exist():
        pass
    else:
        create_table()
    key = _key.decode('utf8')
    for a in arr:
        if len(a)<2:
            continue
        # conn.execute("INSERT INTO RAWDATA (NAME,VALUE,STIME)\
        #         VALUES (?, ?, ?)", (key, a[0], a[1]))
        new_data = Data(device_id=get_dev_id(dev_name),
                        tag_id=get_tag_id(tag_name),
                        value=a[0],
                        stime=a[1],
                        )
        session.add(new_data)
        logger.debug("INSERT to DB::: NAME:{} VALUE:{} STIME:{}".format(key, a[0], a[1]))
    #conn.commit()
    session.commit()
    logger.info("Records for {} key created successfully".format(_key))
    conn.close()


def create_table():
    #import sqlite3
    conn = sqlite3.connect(db_name)
    conn.execute('''CREATE TABLE RAWDATA(
   ID INTEGER PRIMARY KEY   AUTOINCREMENT,
   NAME           TEXT      NOT NULL,
   VALUE          REAL      NOT NULL,
   STIME          CHAR(50));''')
    conn.close()
    print "create db {}".format(db_name)


def check_table_exist():
    #import sqlite3
    conn = sqlite3.connect(db_name)
    c1 = conn.execute("SELECT * FROM sqlite_master WHERE name ='RAWDATA' and type='table';")
    f1 = c1.fetchall()
    if f1:
        return True
    else:
        return False


if __name__ == '__main__':
    logger.debug('Staring mc2mb.......')

    # connect to db
    # get table device, tag
    conn = sqlite3.connect(db_name)
    dd = conn.execute("SELECT * FROM device;").fetchall()
    tt = conn.execute("SELECT * FROM tag;").fetchall()
    conn.close()

    # transform to better search
    import numpy as np
    dd = np.array(dd)
    tt = np.array(tt)

    def get_dev_id(name):
        return dd[dd[:, 2] == name][0, 0]

    def get_tag_id(name):
        return tt[tt[:, 2] == name][0, 0]

    # prepare connection to write to data table
    Base.metadata.bind = engine
    from sqlalchemy.orm import sessionmaker
    DBSession = sessionmaker()
    DBSession.bind = engine
    session = DBSession()

    from time import sleep
    import yaml_data
    all_arc_keys_name = yaml_data.get_all_key_names_special(2)
    for _ in range(3):
        for key in all_arc_keys_name:
            dev_name, tag_name = key.split('.')[1:3]
            for _ in xrange(5):
                val = mc.get(key)
                if val is None:
                    continue
                if mc.cas(key, ''):
                    arr = get_data_array(val)
                    if arr is not None:
                        try:
                            write_data_to_db(key, arr)
                        except:
                            logger.error('smthing wrong with db!')
                    break
        sleep(15)
