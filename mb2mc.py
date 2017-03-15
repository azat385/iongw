#!/usr/bin/python
# -*- coding: utf-8 -*-
from pymodbus.client.sync import ModbusSerialClient as ModbusClient
import yaml_data
from struct import pack, unpack
import memcache
from datetime import datetime

mc = memcache.Client(['127.0.0.1:11211'], debug=0)
# mc.flush_all()

client = ModbusClient(method="rtu", port="/dev/ttyUSB0", baudrate=19200,
                      parity='E', stopbits=1, timeout=2)
client.connect()

server = yaml_data.get_server_name()

for slave_id, slave_name in yaml_data.get_slave_info():
    for rr in yaml_data.get_all_requests():
        reg_start, reg_len = yaml_data.rr_start_and_len(rr)
        reg_type = yaml_data.rr_data(rr)
        reg_data = yaml_data.rr_data(rr)
        try:
            rr = client.read_holding_registers(reg_start - 1, reg_len, unit=slave_id)
            str_tstamp = str(datetime.now())
            d = rr.registers
            if reg_type == 'flt32':
                values = [round(f * 1000, 2) for f in
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
            print 'id={} error while reading'.format(slave_id)
        else:
            for data in reg_data:
                pos, name, need_save = data[0:3]
                if len(data)>3:
                    save_chg_value = data[3]
                    save_chq_time = data[4]
                key_name_now, key_name_arc = yaml_data.\
                    form_key_name(server, slave_name, name,)
                value = values[pos]
                key_value = yaml_data.form_key_value(value, str_tstamp)
                if need_save:
                    pass
                else:
                    mc.set(key=key_name_now,
                           val=key_value,
                           time=0)
        finally:
            pass

client.close()




'''
ids = range(2,9)[::-1]
for id in ids:
    try:
        rr = client.read_holding_registers(rq[0]-1, rq[1], unit=id)
        d = rr.registers
        #print "id=",id,[hex(i) for i in rr.registers]
        print 'id={} {}'.format(id, rq[2]),[round(f, 2) for f in unpack('>{}f'.format(len(d)/2), pack('>{}H'.format(len(d)),*d))]
    except AttributeError:
        print 'id={} error while reading'.format(id)

client.close()

# float32
# all instant values
d = client.read_holding_registers(3000-1, 112, unit=8).registers
print [round(f, 2) for f in unpack('>{}f'.format(len(d)/2), pack('>{}H'.format(len(d)),*d))]

float32 = [0,1,2,5,10,11,12,13,14,15,16,18,27,28,29,30,34,38,42,55]
r = range(3000,3110+2,2)
[r[i] for i in float32]

int64 = [0,1,4,5,13,17]
r = range(3204,3272+4,4)
w = [r[i] for i in int64]

# all cumulative
# float32
d = client.read_holding_registers(45100-1, 28, unit=8).registers
vf = [round(f*1000, 2) for f in unpack('>{}f'.format(len(d)/2), pack('>{}H'.format(len(d)),*d))]
# int64
d = client.read_holding_registers(3204-1, 72, unit=8).registers
v = [f for f in unpack('>{}q'.format(len(d)/4), pack('>{}H'.format(len(d)),*d))]
vi = [v[i] for i in int64]
d = client.read_holding_registers(3518-1, 12, unit=8).registers
vi += [f for f in unpack('>{}q'.format(len(d)/4), pack('>{}H'.format(len(d)),*d))]

for i in range(len(w)):
    print "{} {}".format(w[i], vi[i])

# d = [16828, 17357, 7453, 17356]

from struct import pack, unpack
data_list = list (unpack('>{}f'.format(len(d)/2), pack('>{}H'.format(len(d)),*d)))

from pymodbus.constants import Endian
from pymodbus.payload import BinaryPayloadDecoder
decoder = BinaryPayloadDecoder.fromRegisters(rr.registers, endian=Endian.Little)
print decoder.decode_32bit_float()

sign = ["", "<", ">"]
def try_unpack(d1,d2):
    for s1 in sign:
        for s2 in sign:
            print unpack('{}f'.format(s1), pack('{}2H'.format(s2), d1, d2))
    print "swap words"
    d1,d2 = d2,d1
    for s1 in sign:
        for s2 in sign:
            print unpack('{}f'.format(s1), pack('{}2H'.format(s2), d1, d2))

try_unpack(d[0],d[1])
try_unpack(d[1],d[2])
try_unpack(d[2],d[3])


data_rq = [
    [3000, 6, 'current'],
    [3020, 18,'voltage'],
    [3054, 6, 'power A,B,C'],
    [3060, 2, 'Pactive'],
    [3068, 2, 'Preactive'],
    [3076, 2, 'Pwhole'],
    [3084, 2, 'CosPhi'],
    [3110, 2, 'Frequency'],]

'''