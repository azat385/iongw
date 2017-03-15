#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import yaml

with open("data.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

key_separator = cfg['mc']['key_separator']
value_separator = cfg['mc']['value_separator']
append_value_separator = cfg['mc']['append_value_separator']
ending = cfg['mc']['key_ending']


server = cfg['name']
id_data = cfg['modbus']['id_data']
id_data = [tuple(i) for i in id_data]
id_data = np.array(id_data,
                   dtype=[('id', 'i4'), ('name', 'S20')]
                   )
print id_data
#tag = [r['name'] for r in cfg['modbus']['rr']]
#tag = [l for tl in tag for l in tl]

def get_server_name():
    return cfg['name']

def get_slave_id():
    return id_data['id'].tolist()

def get_slave_name():
    return id_data['name'].tolist()

def get_slave_info():
    return cfg['modbus']['id_data']

def get_all_requests():
    return cfg['modbus']['rr']

def rr_start_and_len(r):
    return r['start_and_len']

def rr_type(r):
    return r['type']

def rr_data(r):
    return r['data']

def form_key_name(*args):
    name = "".join(
        ["{}{}".format(a, key_separator)
         for a in args
         ]
    )
    return ["{}{}".format(name, e) for e in ending]

def form_key_value(*args):
    return "{}{}{}".format(args[0], value_separator, args[1])

if __name__ == '__main__':
    print "start checks"
    #print get_slave_id()
    #print get_slave_name()
    print form_key_name('sss', 'dddd', 'name')
    print form_key_value(555, 'strtime')
