#!/usr/bin/python
# -*- coding: utf-8 -*-

import yaml
from cPickle import dumps, loads
import pprint


pp = pprint.PrettyPrinter(width=500)

with open("data.yaml", 'r') as ymlfile:
    cfg = yaml.load(ymlfile)

key_separator = cfg['mc']['key_separator']
value_separator = cfg['mc']['value_separator']
append_value_separator = cfg['mc']['append_value_separator']
ending = cfg['mc']['key_ending']
server_name, server_desc = cfg['name']
id_data_list = cfg['modbus']['id_data']
rr_list = cfg['modbus']['rr']

# pp.pprint(cfg)


def form_key_name(*args):
    name = "".join(
        ["{}{}".format(a, key_separator)
         for a in args
         ]
    )
    lst = ["{}{}".format(name, e) for e in ending]
    return lst


def form_one_device(id_data, rr_ll, s_name=server_name):
    rr_l = loads(dumps(rr_ll, -1))
    std_dic = {"id_data": list(id_data),
               "rr": rr_l}
    # for r,rr in enumerate(rr_l):
    #     std_dic["rr"][r]["start_and_len"]=list(rr["start_and_len"])
    #     std_dic["rr"][r]["data"] = list(rr["data"])

    # device_name = std_dic["id_data"][1]
    # for r,_ in enumerate(std_dic["rr"]):
    #     for d, __ in enumerate(std_dic["rr"][r]["data"]):
    #         tag_name = std_dic["rr"][r]["data"][d][1]
    #         std_dic["rr"][r]["data"][d][1] = form_key_name(s_name,
    #                                                     device_name,
    #                                                     tag_name)
    device_name = std_dic["id_data"][1]
    for rr in std_dic["rr"]:
        for dd in rr["data"]:
            tag_name = dd[1]
            dd[1] = form_key_name(s_name,
                                  device_name,
                                  tag_name)
    return std_dic


whole_data = [form_one_device(id_data=id_data,
                              rr_ll=rr_list,
                              s_name=server_name)
              for id_data in id_data_list]


# id_data = cfg['modbus']['id_data']
# id_data = [tuple(i) for i in id_data]
# id_data = np.array(id_data,
#                    dtype=[('id', 'i4'), ('name', 'S20')]
#                    )
# print id_data
#tag = [r['name'] for r in cfg['modbus']['rr']]
#tag = [l for tl in tag for l in tl]
def get_all_key_names():
    keys = []
    for dev in whole_data:
        for rr in dev['rr']:
            keys += [d[1] for d in rr['data']]
    return keys

def get_all_key_names_special(pos=0):
    return [d[pos] for d in get_all_key_names()]

def get_slave_info(dev):
    return dev['id_data']

def get_slave_id(dev):
    return get_slave_info(dev)[0]

def get_all_requests(dev):
    return dev['rr']

def rr_start_and_len(r):
    return r['start_and_len']

def rr_type(r):
    return r['type']

def rr_data(r):
    return r['data']

def form_key_value(*args):
    # type: (object) -> object
    return "{}{}{}".format(args[0], value_separator, args[1])


whole_tag = [rr_data(rr) for rr in rr_list]

whole_tag = [item for sublist in whole_tag for item in sublist]

if __name__ == '__main__':
    print "start checks"
    #pp.pprint(whole_data)
    # pp.pprint(get_all_key_names_special(2))
    pp.pprint(whole_tag)
    print len(whole_tag)
    #for t in whole_tag:
    #    print len(t)
    #print len(get_all_key_names_special(2))
    #print get_slave_id()
    #print get_slave_name()
    # with open("test_alias.yaml", 'r') as ymlfile:
    #     cfg_test = yaml.load(ymlfile)
    #
    # import pprint
    #
    # pp = pprint.PrettyPrinter(width=120)
    # pp.pprint(cfg_test)
