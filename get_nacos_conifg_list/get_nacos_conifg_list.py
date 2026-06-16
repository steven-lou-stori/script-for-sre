#!/usr/bin/python3
# -*- coding: utf-8 -*-
"""
获取指定 Nacos 实例中所有 namespace 的配置列表，输出覆盖表格。
列：dataId | namespace1 | namespace2 | ...
每行表示一个配置在各 namespace 中是否存在。
"""
import requests
import json
import csv
import os
import sys
from urllib.parse import urljoin

# --- Nacos 实例地址 ---
# PERF_NACOS_URL = "http://k8s-perf-nacosser-bd4091cc04-62c21ef5bfc6d36f.elb.us-east-1.amazonaws.com:8848"
# QA_NACOS_URL = "http://k8s-default-nacosser-ed942da388-8c93994911b97e04.elb.us-east-1.amazonaws.com:8848"
# COL_NACOS_URL = "http://k8s-colombia-nacoslb-4dd91d43a3-4179efdbf185a0ec.elb.us-east-1.amazonaws.com:8848"
# BACKUP_NACOS_URL = "http://k8s-default-nacoslb-57d783186e-c706c71a14911c2f.elb.us-west-2.amazonaws.com:8848"
# CORE_NACOS_URL = "http://k8s-default-nacoslb-16955a48f0-97c409efec1a3af4.elb.us-east-1.amazonaws.com:8848"
# PERF_NEW_NACOS_URL = "http://k8s-perf-nacoslb-653798335d-61fe046ff5e73c40.elb.us-east-1.amazonaws.com:8848"
# QA_NEW_NACOS_URL = "http://k8s-default-nacoslb-515c162cf7-05805e9de00d8f32.elb.us-east-1.amazonaws.com:8848"
# BACKUP_NEW_NACOS_URL = "http://k8s-default-nacoslb-63ae6fcc63-0f8f765d40ab8d33.elb.us-west-2.amazonaws.com:8848"
# CORE_NEW_NACOS_URL = "http://k8s-default-nacoslb-17bfd1b9c3-0252a9ff2a3e4fb7.elb.us-east-1.amazonaws.com:8848"
# PERF_BK_NACOS_URL = "http://k8s-perf-nacoslb-c2db331aa6-cce453735cc93f2f.elb.us-west-2.amazonaws.com:8848"
DEV_NACOS_URL = "http://nacos.storicarddev.com:8848"
QA_NACOS_URL = "http://nacos.storicard-qa.com:8848"


# --- 目标 Nacos 实例 ---
TARGET_NACOS = QA_NACOS_URL   # 修改此处切换要查询的 Nacos 实例

# --- 认证与参数 ---
NACOS_USERNAME = "nacos"
NACOS_PASSWORD = "nacos"
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), "nacos_all_ns_configs.csv")

# --- 请求日志开关 ---
VERBOSE = False  # 设为 True 可打印每个 API 请求的 URL


class NacosAPI:
    """Nacos Open API 的简易封装"""

    def __init__(self, server_url: str, username: str = NACOS_USERNAME, password: str = NACOS_PASSWORD):
        self.server_url = server_url.rstrip('/')
        self.session = requests.Session()
        self.session.auth = (username, password)
        self.session.headers['Content-Type'] = 'application/json'

    def list_namespaces(self) -> list[dict]:
        """列出所有 namespace；返回 [{"namespace": "xxx", "namespaceShowName": "xxx"}, ...]"""
        url = urljoin(self.server_url + '/', 'nacos/v1/console/namespaces')
        if VERBOSE:
            print(f"[API] GET {url}")
        resp = self.session.get(url, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if body.get('code') != 200:
            raise Exception(f"list namespaces failed: {body.get('message', body)}")
        return body.get('data', [])

    def list_configs(self, namespace_id: str = '', page_size: int = 9999) -> list[dict]:
        """获取某 namespace 下的全部配置；返回 [{"dataId": "xxx", "group": "xxx"}, ...]"""
        url = urljoin(self.server_url + '/', 'nacos/v1/cs/configs')
        params = {
            'search': 'blur',
            'dataId': '',           # Nacos API 必需参数，留空表示匹配所有
            'group': '',            # Nacos API 必需参数，留空表示匹配所有
            'pageNo': 1,
            'pageSize': page_size,
            'tenant': namespace_id,
        }
        if VERBOSE:
            print(f"[API] GET {url}?tenant={namespace_id}")
        resp = self.session.get(url, params=params, timeout=60)
        resp.raise_for_status()
        body = resp.json()
        return body.get('pageItems', [])


def build_coverage(api: NacosAPI) -> dict:
    """
    遍历所有 namespace，返回：
      coverage = {
          (dataId, group): {ns_show_name1, ns_show_name2, ...}
      }
    其中 key 通常为 "dataId"，但 group 也纳入维度（避免不同 group 同名 dataId 混淆）
    """
    print(f"[*] 连接 Nacos: {api.server_url}")
    print("[*] 正在获取 namespace 列表...")
    namespaces = api.list_namespaces()

    # 默认公共 namespace
    ns_list = [('public', '')]  # (show_name, namespace_id)
    for ns in namespaces:
        ns_id = ns.get('namespace', '')
        ns_show = ns.get('namespaceShowName', ns_id)
        if ns_id != '':  # 避免重复添加 public
            ns_list.append((ns_show, ns_id))

    print(f"[*] 共发现 {len(ns_list)} 个 namespace")

    coverage = {}  # key: (dataId, group), value: set of ns_show_name

    for idx, (ns_show, ns_id) in enumerate(ns_list):
        print(f"[*] [{idx+1}/{len(ns_list)}] 正在拉取 namespace: {ns_show} (id={ns_id}) ...", end=' ')
        sys.stdout.flush()
        try:
            configs = api.list_configs(namespace_id=ns_id)
            print(f"{len(configs)} 条配置")
        except Exception as e:
            print(f"失败: {e}")
            continue

        for cfg in configs:
            data_id = cfg.get('dataId', '')
            group = cfg.get('group', 'DEFAULT_GROUP')
            key = (data_id, group)
            if key not in coverage:
                coverage[key] = set()
            coverage[key].add(ns_show)

    return coverage


def print_table(coverage: dict, ns_names: list[str]):
    """终端打印对齐表格"""
    max_code = max((len(f'{k[1]}|{k[0]}') for k in coverage.keys()), default=10)
    col_widths = [max_code] + [max(len(n), 4) for n in ns_names]

    def fmt_row(vals):
        parts = [v.ljust(w) for v, w in zip(vals, col_widths)]
        return ' | '.join(parts)

    header = ['配置code (group|dataId)'] + ns_names
    sep = '-+-'.join('-' * w for w in col_widths)

    print(fmt_row(header))
    print(sep)

    for (data_id, group), ns_set in sorted(coverage.items()):
        row = [f'{group}|{data_id}']
        for ns in ns_names:
            row.append('✓' if ns in ns_set else '-')
        print(fmt_row(row))

    print(sep)
    print(f'\n共 {len(coverage)} 条配置')


def print_summary(coverage: dict, ns_names: list[str]):
    """打印各 namespace 配置数统计"""
    print('\n--- 各 namespace 配置数量统计 ---')
    ns_count = {n: 0 for n in ns_names}
    for ns_set in coverage.values():
        for ns in ns_names:
            if ns in ns_set:
                ns_count[ns] += 1
    total = len(coverage)
    for ns in ns_names:
        cnt = ns_count[ns]
        pct = cnt * 100 // total if total > 0 else 0
        print(f'  {ns}: {cnt} / {total} = {pct}%')


def export_csv(coverage: dict, ns_names: list[str], filepath: str):
    """导出 CSV"""
    with open(filepath, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['配置code'] + ns_names)
        for (data_id, group), ns_set in sorted(coverage.items()):
            row = [f'{group}|{data_id}']
            for ns in ns_names:
                row.append('Y' if ns in ns_set else 'N')
            writer.writerow(row)
    print(f'\nCSV 已导出: {filepath}')


def main():
    api = NacosAPI(TARGET_NACOS)
    coverage = build_coverage(api)

    # 收集所有 namespace 名称，按字母排序
    all_ns = sorted(set().union(*coverage.values())) if coverage else []
    if not all_ns:
        print("[!] 未获取到任何配置，请检查 Nacos 连接和认证信息。")
        sys.exit(1)

    print('\n' + '=' * 120)
    print_table(coverage, all_ns)
    print_summary(coverage, all_ns)
    print('=' * 120)

    export_csv(coverage, all_ns, OUTPUT_CSV)


if __name__ == "__main__":
    main()