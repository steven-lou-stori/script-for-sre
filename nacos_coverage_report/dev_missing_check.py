"""
以 nacos_env_coverage.csv 为基准，找出 DEV=N 且 PROD=Y 的配置。
这些就是 PROD 有但 DEV 还没加的配置（过滤 gray 行）。
"""
import csv
import json
import os

BASE = os.path.dirname(__file__)
RECORDED = os.path.join(BASE, 'nacos_env_coverage.csv')
APP_JSON = os.path.join(BASE, 'app.json')
OUTPUT = os.path.join(BASE, 'dev_need_add_configs.csv')


def load_owner_map():
    """从 app.json 加载 app_name -> owner 映射"""
    with open(APP_JSON, 'r', encoding='utf-8') as f:
        apps = json.load(f)
    return {app['app_name']: app.get('owner', '') for app in apps}


def main():
    owner_map = load_owner_map()
    print(f"[*] 已加载 app_name -> owner 映射: {len(owner_map)} 条")

    # 1. 加载已记录配置（过滤 gray 行），找出 DEV=N 且 PROD=Y 的配置
    dev_n_list = []  # [{app_name, code, owner}]
    with open(RECORDED, 'r', encoding='utf-8-sig') as f:
        for row in csv.DictReader(f):
            code = row['配置code']
            if 'gray' in code.lower():
                continue
            if row.get('DEV', 'N').upper() == 'N' and row.get('PROD', 'N').upper() == 'Y':
                app = row['app_name']
                dev_n_list.append({
                    'app_name': app,
                    'code': code,
                    'owner': owner_map.get(app, ''),
                })

    print(f"[*] DEV=N 且 PROD=Y 配置数（去gray）: {len(dev_n_list)}")

    if not dev_n_list:
        print("\n[*] 所有配置 DEV 均已为 Y。")
        return

    # 2. 输出表格
    dev_n_list.sort(key=lambda r: r['code'])
    rows = [{'app_name': r['app_name'], 'code': r['code'], 'owner': r['owner'], 'status': 'DEV=N，需添加'} for r in dev_n_list]

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
    print(f'\n共 {len(rows)} 条配置 DEV=N 且 PROD=Y，需添加到 DEV 环境')

    # 3. 导出 CSV
    with open(OUTPUT, 'w', encoding='utf-8-sig', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['app_name', '配置code', 'owner', 'DEV_namespace', '操作'])
        writer.writeheader()
        for r in rows:
            writer.writerow({
                'app_name': r['app_name'],
                '配置code': r['code'],
                'owner': r['owner'],
                'DEV_namespace': 'public',
                '操作': r['status'],
            })
    print(f'CSV 已导出: {OUTPUT}')


if __name__ == '__main__':
    main()
