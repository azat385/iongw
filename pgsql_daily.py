# -*- coding: utf-8 -*-

import logging
from logging.handlers import RotatingFileHandler

# format the log entries
formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

handler = RotatingFileHandler('pgsql_daily.log',
                              mode='a',
                              maxBytes=20*1024*1024,
                              backupCount=5,
                              encoding=None,
                              delay=0)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)

from pgsql_hourly import Data, Device, Tag, Base, Hourly, Daily, session, \
                         gw_list, device_list, tag_list

from sqlalchemy import types, and_, select
from sqlalchemy.sql import func, cast

hh = func.date_part('hour',cast(Hourly.stime, types.DateTime)).label('h')
dd = func.date_part('day',cast(Hourly.stime, types.DateTime)).label('d')


import dateutil.parser
def my_tstamp_strip(t):
    return dateutil.parser.parse(t).date().isoformat()
    #datetime.strptime(t,"%Y-%m-%d %H:%M:%S.%f").strftime("%Y-%m-%d %H:00")


def get_daily_last_row_stime():
    # get last row time stamp
    last_record = session.query(Daily).filter(
        and_(
            Daily.gateway==g,
            Daily.device==d,
            Daily.tag==t,
        )
    ).order_by(Daily.id.desc()).first()
    if last_record is None:
        stime = "2017-00-00 00:00"
        logger.info("INITIAL TIME IS USED")
    else:
        stime = last_record.stime

    logger.debug("last_record_stime: {}".format(stime))
    return stime


def add_data_daily(required):
    r_add = 0
    for i,[r_data,r_day] in enumerate(required):
        if i==0:
            r_data_prev = r_data
            continue
        new_record = Daily(gateway=g,
                          device=d,
                          tag=t,
                          value=r_data.start_data.value-r_data_prev.start_data.value,
                          stime=my_tstamp_strip(r_data.stime),
                          start_data=r_data_prev.start_data,
                          end_data=r_data.start_data,
                         )
        # print new_record
        session.add(new_record)
        r_add += 1
        r_data_prev = r_data
    session.commit()
    return r_add


def one_tag_shot(g, d, t):
    sql_limit=1000
    i = 0
    rows_added = 0
    while i<20:
        last_record_stime = get_daily_last_row_stime()
        main_query = session.query(Hourly, dd, hh).filter(
            and_(
                Hourly.gateway==g,
                Hourly.device==d,
                Hourly.tag==t,
                hh<5,
                Hourly.stime>last_record_stime,
            )
        ).order_by(Hourly.id)

        records = main_query.limit(sql_limit).all()
        print records

        if records is None:
            logger.info("NOTHING TO ADD!!!!")
            break
        else:
            # sort duplicate hour data to required
            required = []
            r_day_prev = None
            for r_data, r_day, r_hour in records:
                if r_day != r_day_prev:
                    print r_data, r_day, r_hour
                    required.append([r_data, r_day])
                    r_day_prev = r_day
                else:
                    continue
            logger.debug("len(required):".format(len(required)))

            # add data to table
            if len(required) > 1:
                logger.debug("ADD TO HOURLY TABLE")
                rows_added += add_data_daily(required)
            else:
                logger.debug("TOO LESS DATA, cant add")
                break
        logger.info("{}.{}.{} rows_added: {}".format(g.name, d.name, t.name, rows_added))
        return rows_added
    return rows_added


if __name__ == '__main__':
    logger.info("Start process...")
    for i, t in enumerate(tag_list):
        logger.debug("Tags count:{} {}".format(i+1,t.name))

    rows_count = 0
    for g in gw_list:
        for d in device_list:
            for t in tag_list:
                rows_count += one_tag_shot(g, d, t)

    logger.debug("Daily table count: {}".format(session.query(Daily).count()))
    logger.info('Totaly added rows: {}'.format(rows_count))