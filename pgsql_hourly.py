# -*- coding: utf-8 -*-

import logging
from logging.handlers import RotatingFileHandler

# format the log entries
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

handler = RotatingFileHandler('pgsql_hourly.log',
                              mode='a',
                              maxBytes=20*1024*1024,
                              backupCount=5,
                              encoding=None,
                              delay=0)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Column, Integer, String, ForeignKey, REAL
from sqlalchemy.orm import relationship
# from sqlalchemy.ext.declarative import declarative_base

from sql_create_table import Data, Device, Tag, Base

from sqlalchemy.sql import func, cast
from sqlalchemy import types, and_, select

class Gateway(Base):
    __tablename__ = 'gateway'
    id = Column(Integer, primary_key=True)
    name = Column(String(50), nullable=False, unique=True)
    desc_short = Column(String(50))
    desc_long = Column(String(250))

    def __repr__(self):
        return "<Gateway(id='{}', name='{}')>".format(self.id, self.name)

    
class Hourly(Base):
    __tablename__ = 'hourly'
    id = Column(Integer, primary_key=True)
    gateway_id = Column(Integer, ForeignKey('gateway.id'))
    gateway = relationship(Gateway)   
    device_id = Column(Integer, ForeignKey('device.id'))
    device = relationship(Device)
    tag_id = Column(Integer, ForeignKey('tag.id'))
    tag = relationship(Tag)
    start_data_id = Column(Integer, ForeignKey('data.id'))
    end_data_id = Column(Integer, ForeignKey('data.id'))
    value = Column(REAL)
    stime = Column(String(50))
    
    start_data = relationship(Data, foreign_keys='Hourly.start_data_id')
    end_data = relationship(Data, foreign_keys='Hourly.end_data_id')
    
    def __repr__(self):
        return "<Hourly(id='{}', value='{}')>".format(self.id, self.value)


class Daily(Base):
    __tablename__ = 'daily'
    id = Column(Integer, primary_key=True)
    gateway_id = Column(Integer, ForeignKey('gateway.id'))
    gateway = relationship(Gateway)
    device_id = Column(Integer, ForeignKey('device.id'))
    device = relationship(Device)
    tag_id = Column(Integer, ForeignKey('tag.id'))
    tag = relationship(Tag)
    start_data_id = Column(Integer, ForeignKey('data.id'))
    end_data_id = Column(Integer, ForeignKey('data.id'))
    value = Column(REAL)
    stime = Column(String(50))

    start_data = relationship(Data, foreign_keys='Daily.start_data_id')
    end_data = relationship(Data, foreign_keys='Daily.end_data_id')

    def __repr__(self):
        return "<Daily(id='{}', value='{}')>".format(self.id, self.value)
    
url = 'postgresql://{}:{}@{}:{}/{}'
url = url.format('dbraw', ',fpflfyys[', 'localhost', 5432, 'dbraw')
engine = create_engine(url, client_encoding='utf8', echo=False)

# Base.metadata.create_all(engine)
# meta = MetaData(bind=engine, reflect=True)

# for table in meta.tables:
#     print table


from sqlalchemy.orm import sessionmaker

DBSession = sessionmaker(bind=engine)
session = DBSession()

session.query(Data).count()

# with engine.connect() as con:
#     rs = con.execute("""SELECT data.stime as minute FROM data LIMIT 5""")
#     for row in rs:
#         print row



from datetime import datetime
def my_tstamp_strip(t):
    return datetime.strptime(t,"%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:00")
# ts_example = u'2017-03-26 22:20:00.159490'
# print my_tstamp_strip(ts_example)


def get_hourly_last_row_stime():
    # get last row time stamp
    last_record = session.query(Hourly).filter(
        and_(
            Hourly.gateway==g,
            Hourly.device==d,
            Hourly.tag==t,
        )
    ).order_by(Hourly.id.desc()).first()
    if last_record is None:
        stime = "2017-00-00 00:00"
        logger.info("INITIAL TIME IS USED")
    else:
        stime = last_record.stime

    logger.debug("last_record_stime: {}".format(stime))
    return stime


def add_data_hourly(required):
    r_add = 0
    for i,[r_data,r_hour] in enumerate(required):
        if i==0:
            r_data_prev = r_data
            continue
        #print r_data, r_hour, r_data.value-r_data_prev.value, my_tstamp_strip(r_data.stime)
        new_hour = Hourly(gateway=g,
                          device=d,
                          tag=t,
                          value=r_data.value-r_data_prev.value,
                          stime=my_tstamp_strip(r_data.stime),
                          start_data=r_data_prev,
                          end_data=r_data,
                         )
        session.add(new_hour)
        r_add += 1
        r_data_prev = r_data
    session.commit()
    return r_add


def one_tag_shot(g, d, t):
    sql_limit=1000
    i = 0
    rows_added = 0
    while i<20:
        last_record_stime = get_hourly_last_row_stime()

        main_query = session.query(Data, h, m).filter(
            and_(
                Data.device==d,
                Data.tag==t,
                m<5,
                Data.stime>last_record_stime,
            )
        ).order_by(Data.id)

        records = main_query.limit(sql_limit).all()
        # print records

        if records is None:
            logger.info("NOTHING TO ADD!!!!")
            break
        else:
            #sort duplicate hour data to required
            required = []
            r_hour_prev = None
            for r_data, r_hour, r_min in records:
                if r_hour!=r_hour_prev:
                    #print r_data, r_hour, r_min
                    required.append([r_data, r_hour])
                    r_hour_prev=r_hour
                else:
                    continue
            logger.debug("len(required):".format(len(required)))

            # add data to table
            if len(required)>1:
                logger.debug("ADD TO HOURLY TABLE")
                rows_added += add_data_hourly(required)
            else:
                logger.debug("TOO LESS DATA, cant add")
                break
    logger.info("{}.{}.{} rows_added: {}".format(g.name, d.name, t.name, rows_added))
    return rows_added


m = func.date_part('minute',cast(Data.stime, types.DateTime)).label('m')
h = func.date_part('hour',cast(Data.stime, types.DateTime)).label('h')


# Here define gateway, device and tags to add hourly procedure
gw_list = session.query(Gateway).all()
device_list = session.query(Device).all()
tag_list = session.query(Tag).filter(Tag.name.ilike('%import%')&~Tag.name.ilike('%part%')).all()


if __name__ == '__main__':
    logger.info("Start process...")
    for i, t in enumerate(tag_list):
        logger.debug("Tags count:{} {}".format(i+1,t.name))

    rows_count = 0
    for g in gw_list:
        for d in device_list:
            for t in tag_list:
                rows_count += one_tag_shot(g, d, t)

    logger.debug("Hourly table count: {}".format(session.query(Hourly).count()))
    logger.info('Totaly added rows: {}'.format(rows_count))

# last_record = session.query(Hourly).filter(
#     and_(
#         Hourly.gateway==g,
#         Hourly.device==d,
#         Hourly.tag==t,
#     )
# ).order_by(Hourly.id.desc()).all()
#
# print "len(last_record):", len(last_record)
# for i,r in enumerate(last_record[:5]):
#     print i, r,r.stime

# session.query(Hourly).delete()
# session.commit()

