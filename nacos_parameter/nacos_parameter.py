#!/usr/bin/python3
# -*- coding: utf-8 -*-
import jproperties
import shutil
import nacos
import os
import re
PERF_NACOS_URL = "http://k8s-perf-nacosser-bd4091cc04-62c21ef5bfc6d36f.elb.us-east-1.amazonaws.com:8848"
QA_NACOS_URL = "http://k8s-default-nacosser-ed942da388-8c93994911b97e04.elb.us-east-1.amazonaws.com:8848"
COL_NACOS_URL = "http://k8s-colombia-nacoslb-4dd91d43a3-4179efdbf185a0ec.elb.us-east-1.amazonaws.com:8848"
BACKUP_NACOS_URL ="http://k8s-default-nacoslb-57d783186e-c706c71a14911c2f.elb.us-west-2.amazonaws.com:8848"
CORE_NACOS_URL="http://k8s-default-nacoslb-16955a48f0-97c409efec1a3af4.elb.us-east-1.amazonaws.com:8848"
PERF_NEW_NACOS_URL = "http://k8s-perf-nacoslb-653798335d-61fe046ff5e73c40.elb.us-east-1.amazonaws.com:8848"
QA_NEW_NACOS_URL = "http://k8s-default-nacoslb-515c162cf7-05805e9de00d8f32.elb.us-east-1.amazonaws.com:8848"
BACKUP_NEW_NACOS_URL ="http://k8s-default-nacoslb-63ae6fcc63-0f8f765d40ab8d33.elb.us-west-2.amazonaws.com:8848"
CORE_NEW_NACOS_URL="http://k8s-default-nacoslb-17bfd1b9c3-0252a9ff2a3e4fb7.elb.us-east-1.amazonaws.com:8848"
PERF_BK_NACOS_URL = "http://k8s-perf-nacoslb-c2db331aa6-cce453735cc93f2f.elb.us-west-2.amazonaws.com:8848"
SRC_NACOS = PERF_NEW_NACOS_URL
SRC_NS = None
DEST_NACOS = PERF_BK_NACOS_URL
DEST_NS = None
TMP_DIR = "nacosdiff"
class FILTER:
    def __init__(self) -> None:
        self.filter_funcs = []
    def add_filter(self, func):
        self.filter_funcs.append(func)
        return func
    def run_filter(self, s):
        for func in self.filter_funcs:
            s = func(s)
        return s
filter = FILTER()
@filter.add_filter
def rm_comment_and_blanklines(s):
    lines = s.split("\n")
    res_list = []
    for line in lines:
        try:
            line = line.strip('\n\r\t ')
            if line[0] != "#":
                res_list.append(line)
        except:
            pass
    return '\n'.join(res_list)
@filter.add_filter
def strip_kvs(s):
    s = s.replace("\\\n", "")
    lines = s.split("\n")
    res_list = []
    for line in lines:
        try:
            key, val = line.split("=",maxsplit=1)
            key = key.strip()
            val = val.strip()
            line = key + "=" + val
        except:
            pass
        res_list.append(line)
    return '\n'.join(res_list)
def jproperties_parser(s):
    s = filter.run_filter(s)
    res = {}
    lines = s.split("\n")
    for line in lines:
        try:
            key, val = line.split("=",maxsplit=1)
            key = key.strip()
            val = val.strip()
            res[key] = val
        except:
            pass 
    return res
def get_config(url, ns):   
    nacos_client = nacos.NacosClient(url,namespace=ns, username="nacos", password="nacos")
    res = {}
    rt = nacos_client.get_configs()
    for entry in rt["pageItems"]:
        res[entry["dataId"]] = jproperties_parser(entry["content"])
    return res
def main():
    left_json = get_config(SRC_NACOS, SRC_NS)
    right_json = get_config(DEST_NACOS, DEST_NS)
    res = {}
    diff = []
    all_data_id_set = set(left_json.keys())
    all_data_id_set.update(set(right_json.keys()))
    for data_id in all_data_id_set:
        res[data_id] = {}
        left_data = left_json.get(data_id, {})
        right_data = right_json.get(data_id, {})
        all_key_set = set(left_data.keys())
        all_key_set.update(set(right_data.keys()))
        for key in all_key_set:
            res[data_id][key] = {}
            res[data_id][key]["left"] = left_json.get(data_id, {}).get(key, "_no_data")
            res[data_id][key]["right"] = right_json.get(data_id, {}).get(key, "_no_data")
            if res[data_id][key]["left"] != res[data_id][key]["right"]:
                diff.append((data_id, key, res[data_id][key]["left"], res[data_id][key]["right"]))
    for entry in diff:
        print("\t". join(entry))         
    return res, diff
if __name__ == "__main__":
    main()