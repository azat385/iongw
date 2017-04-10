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
                                   interval=48,
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

value_separator = yaml_data.value_separator  # ;
append_value_separator = yaml_data.append_value_separator  # ;;;


def check_time_passed(t1, t2, delta_sec=120):
    t1 = datetime.strptime(t1,"%Y-%m-%d %H:%M:%S.%f")
    t2 = datetime.strptime(t2,"%Y-%m-%d %H:%M:%S.%f")
    dif = t2-t1
    dif_sec = abs(dif.total_seconds())
    return dif_sec >= delta_sec
    #print "dif time= {} delta= {}".format(dif_sec,deltaSec)


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
                              unpack('>{}f'.format(len(d) / 2),
                                     pack('>{}H'.format(len(d)), *d)
                                     )
                              ]
                if reg_type == 'int64':
                    values = [f for f in
                              unpack('>{}q'.format(len(d) / 4),
                                     pack('>{}H'.format(len(d)), *d)
                                     )
                              ]
            except AttributeError:
                logger.error('id={} error while reading'.format(slave_id))
            else:
                for data in reg_data:
                    pos, name_list, need_save = data[0:3]
                    key_name_now, key_name_lst, key_name_arc = name_list
                    if len(data) > 3:
                        save_delta_value, save_delta_time = data[3]
                    current_value = values[pos]
                    key_value = yaml_data.form_key_value(current_value, str_tstamp)

                    # set now value
                    mc.set(key=key_name_now, val=key_value, time=0)
                    logger.debug("Write to mc ONLINE {}:{}".format(key_name_now, key_value))
                    # if True to save
                    if need_save:
                        last_saved_value_str = mc.get(key_name_lst)
                        if last_saved_value_str is None:
                            logger.debug("Create to mc ONLINE {}:{}".format(key_name_now, key_value))
                            mc.set(key_name_lst, key_value)
                            last_saved_value_str = mc.get(key_name_lst)
                        # decompose last saved value
                        last_saved_value, last_saved_time = tuple(last_saved_value_str.split(value_separator))[:2]
                        last_saved_value = float(last_saved_value)
                        if (abs(last_saved_value - current_value) > save_delta_value) \
                                or (check_time_passed(str_tstamp, last_saved_time, save_delta_time)):
                            logger.debug("Write to mc LAST {}:{}".format(key_name_lst, key_value))
                            mc.set(key_name_lst, key_value)
                            logger.debug("Append to mc ARC {}:{}".format(key_name_lst, key_value))
                            if mc.append(key_name_arc, "{}{}".format(append_value_separator, key_value)) is False:
                                mc.set(key_name_arc, "{}{}".format(append_value_separator, key_value))
            finally:
                pass

client.close()
