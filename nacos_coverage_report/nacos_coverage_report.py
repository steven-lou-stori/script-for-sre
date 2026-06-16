"""
从 nacos 配置 JSON 文件生成环境覆盖统计表。
列：app_name | 配置code | DEV | QA | BACKUP | GRAY | PROD
每行表示一个 app_name+code 组合在各环境是否有配置数据。
"""

import json
import sys
import os

# --- 配置 ---
INPUT_FILE = os.path.join(os.path.dirname(__file__), 'nacos_list.json')
OUTPUT_CSV = os.path.join(os.path.dirname(__file__), 'nacos_env_coverage.csv')
ENV_ORDER = ['DEV', 'QA', 'BACKUP', 'GRAY', 'PROD']  # 按此顺序输出列


def load_and_group(filepath: str) -> dict:
    """加载 JSON 并按 (app_name, code) 分组，记录出现过的环境集合"""
    with open(filepath, 'r', encoding='utf-8') as f:
        data = json.load(f)

    coverage = {}  # key: (app_name, code), value: set of envs
    for item in data:
        env = item.get('env', '').upper()
        app = item.get('app_name', '')
        code = item.get('code', '')

        if not app or not code or env not in ENV_ORDER:
            continue  # 跳过不合规数据

        key = (app, code)
        if key not in coverage:
            coverage[key] = set()
        coverage[key].add(env)

    return coverage


def print_table(coverage: dict):
    """终端打印对齐的表格"""
    # 计算列宽
    max_app = max(len(k[0]) for k in coverage.keys())
    max_code = max(len(k[1]) for k in coverage.keys())
    col_widths = [max_app, max_code] + [len(e) for e in ENV_ORDER]

    def fmt_row(vals):
        parts = [v.ljust(w) for v, w in zip(vals, col_widths)]
        return ' | '.join(parts)

    header = ['app_name', '配置code (group|dataId)'] + ENV_ORDER
    sep = '-+-'.join('-' * w for w in col_widths)

    print(fmt_row(header))
    print(sep)

    total = 0
    for (app, code), envs in sorted(coverage.items()):
        row = [app, code]
        for e in ENV_ORDER:
            row.append('✓' if e in envs else '-')
        print(fmt_row(row))
        total += 1

    print(sep)
    print(f'\n共 {total} 组配置 (app_name + code)')

    # 统计覆盖情况
    print('\n--- 各环境覆盖统计 ---')
    env_coverage = {e: 0 for e in ENV_ORDER}
    for envs in coverage.values():
        for e in ENV_ORDER:
            if e in envs:
                env_coverage[e] += 1
    for e in ENV_ORDER:
        print(f'  {e}: {env_coverage[e]} / {total} = {env_coverage[e]*100//total}%')


def export_csv(coverage: dict, filepath: str):
    """导出 CSV 文件"""
    with open(filepath, 'w', encoding='utf-8-sig') as f:
        headers = ['app_name', '配置code (group|dataId)'] + ENV_ORDER
        f.write(','.join(headers) + '\n')
        for (app, code), envs in sorted(coverage.items()):
            vals = [app, code]
            for e in ENV_ORDER:
                vals.append('Y' if e in envs else 'N')
            f.write(','.join(vals) + '\n')
    print(f'\nCSV 文件已导出: {filepath}')


def main():
    if not os.path.exists(INPUT_FILE):
        print(f'[!] 输入文件不存在: {INPUT_FILE}')
        sys.exit(1)

    print(f'[*] 读取文件: {INPUT_FILE}')
    coverage = load_and_group(INPUT_FILE)

    print('\n' + '=' * 80)
    print_table(coverage)
    print('=' * 80)

    export_csv(coverage, OUTPUT_CSV)


if __name__ == '__main__':
    main()
