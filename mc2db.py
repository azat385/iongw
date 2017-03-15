#!/usr/bin/python
# -*- coding: utf-8 -*-

import numpy as np
import yaml

with open("data.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

import pprint
pp = pprint.PrettyPrinter(width=120)
pp.pprint(cfg)


def check_each_rr(rr):
    reg_start, reg_len = rr['start_and_len']
    reg_type = rr['type']
    pos = rr['data']
    name = rr['data']

    if len(pos) <> len(name):
        print "Error: different length of arrays pos:{} name:{}".format(len(pos),len(name))

    if len(name) <> len(set(name)):
        print "Error: duplicate in names"

def check_key_len(names, max_len=240):
    for n in names:
        if len(n)>max_len:
            print "Error: {} is bigger than "

def check_name(d={}):
    if d.has_key('name'):
        return d['name']


#for rr in cfg['modbus']['rr']:
#    check_each_rr(rr)

# separator = cfg['mc']['key_separator']
# server = cfg['name']
# device = cfg['modbus']['name']
# tag = [r['name'] for r in cfg['modbus']['rr']]
# tag = [l for tl in tag for l in tl]
# ending = cfg['mc']['key_ending']

# all_tags = []
# for k_ending in ending:
#     for k_server in server:
#         for k_device in device:
#             for k_tag in tag:
#                 all_tags.append("".join(
#                     ["{}{}".format(k,separator) for k in
#                      [k_server, k_device, k_tag, k_ending]]
#                     )[:-1]
#                 )
# print all_tags


if __name__ == '__main__':
    import yaml_data

    print yaml_data.get_slave_id()

