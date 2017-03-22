#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
import yaml_data
from struct import pack, unpack
import memcache
from datetime import datetime
from time import sleep
import logging
from logging.handlers import TimedRotatingFileHandler

formatter = logging.Formatter('%(asctime)s %(name)s %(levelname)s %(message)s')

handler = TimedRotatingFileHandler('modbus_rtu.log',
                                   when='H',
                                   interval=4,
                                   backupCount=5)
handler.setFormatter(formatter)
logger = logging.getLogger(__name__)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


devs = yaml_data.whole_data
# print (devs)
mc = memcache.Client(['127.0.0.1:11211'], debug=0)
# mc.flush_all()
client = ModbusClient(method="rtu", port="/dev/ttyUSB0", baudrate=19200,
                      parity='E', stopbits=1, timeout=2)
client.connect()
logger.info("Staring infinite loop....")
while True:
    for dev in devs:
        slave_id = yaml_data.get_slave_id(dev)
        for rr in yaml_data.get_all_requests(dev):
            reg_start, reg_len = yaml_data.rr_start_and_len(rr)
            reg_type = yaml_data.rr_type(rr)
            reg_data = yaml_data.rr_data(rr)
            try:
                rr = client.read_holding_registers(reg_start - 1, reg_len, unit=slave_id)
                str_tstamp = str(datetime.now())
                d = rr.registers
                if reg_type == 'flt32':
                    values = [round(f * 1.0, 2) for f in
                              unpack('>{}f'.format(len(d)/2),
                                     pack('>{}H'.format(len(d)), *d)
                                     )
                              ]
                if reg_type == 'int64':
                    values = [f for f in
                              unpack('>{}q'.format(len(d)/4),
                                     pack('>{}H'.format(len(d)),*d)
                                     )
                              ]
            except AttributeError:
                logger.error('id={} error while reading'.format(slave_id))
            else:
                for data in reg_data:
                    pos, name_list, need_save = data[0:3]
                    key_name_now, key_name_lst, key_name_arc= name_list
                    if len(data)>3:
                        save_chg_value = data[3]
                        save_chq_time = data[4]
                    value = values[pos]
                    key_value = yaml_data.form_key_value(value, str_tstamp)
                    if need_save:
                        pass
                    else:
                        mc.set(key=key_name_now,
                               val=key_value,
                               time=0)
                        logger.debug("Write to mc {}:{}".format(key_name_now,key_value))
            finally:
                pass

client.close()

