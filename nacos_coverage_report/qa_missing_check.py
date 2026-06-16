"""
以 nacos_env_coverage.csv 为基准，找出 QA=N 且 PROD=Y 的配置，
并交叉校验 nacos_all_qa_ns_configs.csv（该 code 必须实际存在于 QA Nacos 中）。
"""
import csv
import json
import os

BASE = os.path.dirname(__file__)
RECORDED = os.path.join(BASE, 'nacos_env_coverage.csv')
ACTUAL_QA = os.path.join(BASE, '..', 'get_nacos_conifg_list', 'nacos_all_qa_ns_configs.csv')
APP_JSON = os.path.join(BASE, 'app.json')
OUTPUT = os.path.join(BASE, 'qa_need_add_configs.csv')


def load_owner_map():
    """从 app.json 加载 app_name -> owner 映射"""
    with open(APP_JSON, 'r', encoding='utf-8') as f:
        apps = json.load(f)
    return {app['app_name']: app.get('owner', '') for app in apps}


def load_actual_qa_codes():
    """加载 nacos_all_qa_ns_configs.csv 中所有实际存在的配置code"""
    codes = set()
    with open(ACTUAL_QA, 'r', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            codes.add(row['配置code'])
    return codes


def main():
    owner_map = load_owner_map()
    print(f"[*] 已加载 app_name -> owner 映射: {len(owner_map)} 条")

    # 1. 加载实际 QA Nacos 中存在的配置code
    actual_qa_codes = load_actual_qa_codes()
    print(f"[*] 实际 QA Nacos 配置数: {len(actual_qa_codes)}")

    # 2. 加载已记录配置（过滤 gray 行），找出 QA=N 且 PROD=Y 的配置
    #    且必须实际存在于 QA Nacos 中
    qa_n_list = []  # [{app_name, code, owner}]
    with open(RECORDED, 'r', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            code = row['配置code']
            if 'gray' in code.lower():
                continue
            if code not in actual_qa_codes:
                continue
            if row.get('QA', 'N').upper() == 'N' and row.get('PROD', 'N').upper() == 'Y':
                app = row['app_name']
                qa_n_list.append({
                    'app_name': app,
                    'code': code,
                    'owner': owner_map.get(app, ''),
                })

    print(f"[*] QA=N 且 PROD=Y 且存在于实际 QA Nacos 的配置数: {len(qa_n_list)}")

    if not qa_n_list:
        print("\n[*] 实际QA Nacos中所有配置 QA 均已为 Y。")
        return

    # 2. 输出表格
    qa_n_list.sort(key=lambda r: r['code'])
    rows = [{'app_name': r['app_name'], 'code': r['code'], 'owner': r['owner'], 'status': 'QA=N，需添加'} for r in qa_n_list]

    max_app = max(len(r['app_name']) for r in rows)
    max_code = max(len(r['code']) for r in rows)
    max_owner = max((len(r['owner']) for r in rows), default=0)
    max_stat = max(len(r['status']) for r in rows)

    def fmt_row(vals, widths):
        return ' | '.join(v.ljust(w) for v, w in zip(vals, widths))

    widths = [max_app, max_code, max_owner, max_stat]
    header = fmt_row(['app_name', '配置code (group|dataId)', 'owner', '操作'], widths)
    sep = '-+-'.join('-' * w for w in widths)

    print('\n' + header)
    print(sep)
    for r in rows:
        print(fmt_row([r['app_name'], r['code'], r['owner'], r['status']], widths))
    print(sep)
    print(f'\n共 {len(rows)} 条配置 QA=N 且 PROD=Y，需添加到 QA 环境')

    # 3. 导出 CSV
    with open(OUTPUT, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['app_name', '配置code', 'owner', 'QA_namespace', '操作'])
        writer.writeheader()
        for r in rows:
            writer.writerow({
                'app_name': r['app_name'],
                '配置code': r['code'],
                'owner': r['owner'],
                'QA_namespace': 'public',
                '操作': r['status'],
            })
    print(f'CSV 已导出: {OUTPUT}')


if __name__ == '__main__':
    main()
