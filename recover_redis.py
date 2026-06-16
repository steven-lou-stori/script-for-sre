import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import TimeoutError, ConnectionError
import time
import sys
import json
import os
import base64
import re
from datetime import datetime

# --- 配置信息 ---
REDIS_HOST = "dev-core-banking-redis.lnvbm2.ng.0001.usw2.cache.amazonaws.com"       # 目标 Redis 地址
REDIS_PORT = 6379
REDIS_PWD = None
REDIS_DB = 3          # 恢复到哪个逻辑库（与备份时的 DB 可以不同）

# 恢复来源
BACKUP_DIR = './redis_backups'        # 备份文件所在目录
# 指定要恢复的备份文件前缀，例如 'backup_db3_load_test_keystar_20260610_170600'
# 留空 '' 则自动从 cursor 文件读取，或报错退出
RECOVER_FILE_PREFIX = 'backup_db3_load_test_keystar_20260610_170600'

# 恢复限速与批次参数
RECOVER_BATCH_SIZE = 500   # 每批 pipeline 写入的 key 数量
SLEEP_TIME = 0.1           # 每批写入后的休眠秒数
MAX_RETRIES = 3            # pipeline 最大重试次数
RETRY_BACKOFF_BASE = 0.5   # 重试退避基数（秒）
RETRY_BACKOFF_CAP = 5.0    # 重试退避上限（秒）
SOCKET_TIMEOUT = 10.0      # socket 读写超时（秒）
SOCKET_CONNECT_TIMEOUT = 5.0  # 连接超时（秒）

# 恢复批次控制
MAX_RECOVERY_COUNT = 0          # 最多恢复的记录数（0 表示不限制，恢复所有）
START_RECORD_INDEX = 0          # 从备份文件中的第 N 条记录开始恢复（0=从头开始）
SKIP_EXISTING = False           # 是否跳过目标 Redis 中已存在的 key


# ============================================================
# 辅助类 / 函数
# ============================================================

def ensure_backup_dir():
    """确保备份目录存在"""
    os.makedirs(BACKUP_DIR, exist_ok=True)


def _get_cursor_file_path(base_name):
    """获取恢复进度 cursor 文件路径"""
    safe_name = re.sub(r'[^a-zA-Z0-9_-]', '_', base_name)
    return os.path.join(BACKUP_DIR, f'recover_db{REDIS_DB}_{safe_name}.cursor.json')


def _load_cursor_state(base_name):
    """加载上次恢复的进度；不存在则返回 None"""
    cursor_file = _get_cursor_file_path(base_name)
    if os.path.exists(cursor_file):
        try:
            with open(cursor_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            print(f"[*] 加载上次恢复进度: 已恢复 {state.get('recovered', 0)} 条, "
                  f"文件={state.get('file', 'unknown')}")
            return state
        except Exception as e:
            print(f"[!] 读取 cursor 文件失败: {e}")
    return None


def _save_cursor_state(base_name, recovered, current_file):
    """保存恢复进度"""
    cursor_file = _get_cursor_file_path(base_name)
    state = {
        'recovered': recovered,
        'db': REDIS_DB,
        'file': current_file,
        'timestamp': datetime.now().isoformat(),
    }
    try:
        with open(cursor_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception as e:
        print(f"\n[!] 保存进度文件失败: {e}")


def _find_backup_files(prefix):
    """按前缀查找所有备份 .jsonl 文件，按名称排序"""
    files = []
    for f in os.listdir(BACKUP_DIR):
        if f.startswith(prefix) and f.endswith('.jsonl'):
            files.append(os.path.join(BACKUP_DIR, f))
    # 按文件名排序（part001, part002...）保证顺序正确
    files.sort()
    return files


def create_redis_client():
    """使用连接池创建 Redis 客户端"""
    retry = Retry(ExponentialBackoff(RETRY_BACKOFF_BASE, RETRY_BACKOFF_CAP), MAX_RETRIES)
    pool = redis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PWD,
        db=REDIS_DB,
        decode_responses=False,   # 恢复时也不自动解码，保留原始字节给命令使用
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
        retry=retry,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    return redis.StrictRedis(connection_pool=pool)


# ============================================================
# 值反序列化
# ============================================================

def _deserialize_value(value):
    """
    反向还原 backup_redis.py 中 _serialize_value 的输出。
    __binary__ 标记的值 -> bytes，其他保持原样
    """
    if value is None:
        return None
    if isinstance(value, dict):
        if value.get('__binary__'):
            return base64.b64decode(value['data'])
        return {k: _deserialize_value(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_deserialize_value(item) for item in value]
    return value


# ============================================================
# 恢复一条记录
# ============================================================

def _apply_record(pipe, record, existing_keys):
    """
    根据记录类型构造对应的 Redis 命令，写入 pipeline。
    返回该 key 的名称，用于后续设置 TTL。
    若 SKIP_EXISTING 且 key 已存在，返回 None。
    """
    key = record['key']
    val = _deserialize_value(record['value'])
    typ = record['type']
    ttl_val = record.get('ttl', -1)

    # 跳过已存在的 key
    if SKIP_EXISTING and key in existing_keys:
        return None

    if typ in ('string', 'none'):
        pipe.set(key, val if val is not None else '')
    elif typ == 'hash' and isinstance(val, dict):
        pipe.delete(key)   # 先删再写，保证与原备份一致
        if val:
            pipe.hset(key, mapping=val)
    elif typ == 'list' and isinstance(val, list):
        pipe.delete(key)
        if val:
            pipe.rpush(key, *val)
    elif typ == 'set' and isinstance(val, list):
        pipe.delete(key)
        if val:
            pipe.sadd(key, *val)
    elif typ == 'zset' and isinstance(val, list):
        pipe.delete(key)
        if val:
            # zrange withscores 返回 [[member1, score1], [member2, score2], ...]
            flat = []
            for item in val:
                if isinstance(item, list) and len(item) == 2:
                    flat.append(item[1])   # score
                    flat.append(item[0])   # member
            if flat:
                pipe.zadd(key, dict(zip(flat[1::2], flat[0::2])))
    else:
        print(f"\n[!] 跳过来自文件的不支持类型: {key} (type={typ})")
        return None

    # 设置 TTL
    if ttl_val > 0:
        pipe.expire(key, ttl_val)

    return key


# ============================================================
# 主流程
# ============================================================

def recover():
    r = create_redis_client()
    ensure_backup_dir()

    # --- 确定备份文件 ---
    if RECOVER_FILE_PREFIX:
        prefix = RECOVER_FILE_PREFIX
    else:
        print("[!] 未配置 RECOVER_FILE_PREFIX，请指定要恢复的备份文件前缀。")
        exit(1)

    backup_files = _find_backup_files(prefix)
    if not backup_files:
        print(f"[!] 在 {BACKUP_DIR} 中未找到前缀为 '{prefix}' 的 .jsonl 文件。")
        exit(1)

    print(f"[*] 找到 {len(backup_files)} 个备份文件:")
    for bf in backup_files:
        print(f"    {os.path.basename(bf)}")

    # --- 确定起始位置 ---
    if START_RECORD_INDEX > 0:
        skip_records = START_RECORD_INDEX
        print(f"[*] 使用配置的起始记录索引: {skip_records}")
    else:
        saved_state = _load_cursor_state(prefix)
        if saved_state and saved_state.get('recovered', 0) > 0:
            skip_records = saved_state['recovered']
            print(f"[*] 从上次进度继续，跳过前 {skip_records} 条记录")
        else:
            skip_records = 0

    total_recovered = 0
    total_skipped_existing = 0
    total_skipped_type = 0
    recover_limit = MAX_RECOVERY_COUNT if MAX_RECOVERY_COUNT > 0 else None
    last_file = None

    print(f"[*] 目标逻辑库 (DB): {REDIS_DB}")
    print(f"[*] RECOVER_BATCH_SIZE={RECOVER_BATCH_SIZE}")
    print(f"[*] MAX_RECOVERY_COUNT={MAX_RECOVERY_COUNT if recover_limit else '不限'}")
    print(f"[*] SKIP_EXISTING={SKIP_EXISTING}")
    if skip_records > 0:
        print(f"[*] 跳过前 {skip_records} 条记录后开始恢复")
    print("[*] 提示: 按 Ctrl+C 可以安全停止脚本并查看进度")

    try:
        records_to_skip = skip_records

        for file_path in backup_files:
            last_file = file_path
            print(f"[*] 正在处理: {os.path.basename(file_path)}")

            with open(file_path, 'r', encoding='utf-8') as f:
                # 跳过本文件中需要跳过的记录
                for _ in range(records_to_skip):
                    f.readline()
                if records_to_skip > 0:
                    print(f"[*] 已跳过本文件中前 {records_to_skip} 条记录")
                    records_to_skip = 0

                batch = []
                batch_existing = set()

                for line in f:
                    line = line.strip()
                    if not line:
                        continue

                    try:
                        record = json.loads(line)
                    except json.JSONDecodeError as e:
                        print(f"\n[!] JSON 解析失败: {line[:80]}... 错误: {e}")
                        continue

                    batch.append(record)

                    # 攒够一批了就写入
                    if len(batch) >= RECOVER_BATCH_SIZE:
                        if SKIP_EXISTING:
                            # 批量检查 key 是否存在
                            batch_existing = _check_existing_keys(r, [rec['key'] for rec in batch])

                        recovered, skipped_ex, skipped_ty = _recover_batch(
                            r, batch, batch_existing
                        )
                        total_recovered += recovered
                        total_skipped_existing += skipped_ex
                        total_skipped_type += skipped_ty

                        sys.stdout.write(
                            f"\r[+] 已恢复 {total_recovered} 条"
                            f"{' (跳过已存在' + str(total_skipped_existing) + '个)' if total_skipped_existing else ''}"
                        )
                        sys.stdout.flush()

                        _save_cursor_state(prefix, total_recovered, file_path)
                        time.sleep(SLEEP_TIME)

                        batch = []
                        batch_existing = set()

                        # 达到恢复数量上限
                        if recover_limit and total_recovered >= recover_limit:
                            print(f"\n\n[!] 已达到最大恢复数量 ({recover_limit})，停止。")
                            return

                # 处理文件剩余不足一批的记录
                if batch:
                    if SKIP_EXISTING:
                        batch_existing = _check_existing_keys(r, [rec['key'] for rec in batch])

                    recovered, skipped_ex, skipped_ty = _recover_batch(
                        r, batch, batch_existing
                    )
                    total_recovered += recovered
                    total_skipped_existing += skipped_ex
                    total_skipped_type += skipped_ty

                    sys.stdout.write(
                        f"\r[+] 已恢复 {total_recovered} 条"
                        f"{' (跳过已存在' + str(total_skipped_existing) + '个)' if total_skipped_existing else ''}"
                    )
                    sys.stdout.flush()

                    _save_cursor_state(prefix, total_recovered, file_path)

                    if recover_limit and total_recovered >= recover_limit:
                        print(f"\n\n[!] 已达到最大恢复数量 ({recover_limit})，停止。")
                        return

    except KeyboardInterrupt:
        print("\n\n[!] 检测到 Ctrl+C，正在停止脚本...")
        _save_cursor_state(prefix, total_recovered, last_file or 'unknown')
    except Exception as e:
        print(f"\n[!] 运行出错: {e}")
        _save_cursor_state(prefix, total_recovered, last_file or 'unknown')
    finally:
        print(f"\n[*] 脚本运行结束。")
        print(f"    DB[{REDIS_DB}] 成功恢复数:   {total_recovered}")
        if total_skipped_existing:
            print(f"    DB[{REDIS_DB}] 跳过已存在:   {total_skipped_existing}")
        if total_skipped_type:
            print(f"    DB[{REDIS_DB}] 跳过不支持类型: {total_skipped_type}")
        print("-" * 30)


# ============================================================
# 内部辅助函数
# ============================================================

def _check_existing_keys(r, keys):
    """批量检查 key 是否存在，返回存在的 key 集合"""
    pipe = r.pipeline(transaction=False)
    for k in keys:
        pipe.exists(k)
    results = _pipeline_execute_with_retry(r, pipe)
    existing = set()
    for k, exist_flag in zip(keys, results):
        if exist_flag:
            existing.add(k)
    return existing


def _recover_batch(r, batch_records, existing_keys):
    """
    恢复一批记录到 Redis。
    返回 (recovered_count, skipped_existing_count, skipped_type_count)
    """
    pipe = r.pipeline(transaction=False)
    keys_in_pipe = []
    skipped_existing = 0
    skipped_type = 0

    for record in batch_records:
        result = _apply_record(pipe, record, existing_keys)
        if result is None:
            # 是因为类型不支持还是 key 已存在？
            if SKIP_EXISTING and record['key'] in existing_keys:
                skipped_existing += 1
            else:
                skipped_type += 1
        else:
            keys_in_pipe.append(result)

    if keys_in_pipe:
        _pipeline_execute_with_retry(r, pipe)

    return len(keys_in_pipe), skipped_existing, skipped_type


def _pipeline_execute_with_retry(r, pipe):
    """带重试的 pipeline 执行"""
    for attempt in range(MAX_RETRIES):
        try:
            return pipe.execute()
        except (TimeoutError, ConnectionError) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            print(f"\n[!] Pipeline 超时/连接错误 (尝试 {attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
        except Exception:
            raise


if __name__ == "__main__":
    if not REDIS_HOST:
        print(f"[*] Redis 地址未配置 (REDIS_HOST='{REDIS_HOST}')，请设置。")
        exit(1)
    if not RECOVER_FILE_PREFIX:
        print("[!] 恢复时需要指定 RECOVER_FILE_PREFIX 备份文件前缀。")
        print("    例如: RECOVER_FILE_PREFIX = 'backup_db3_load_test_keystar_20260610_170600'")
        exit(1)

    recover()
