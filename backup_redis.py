import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import TimeoutError, ConnectionError
import time
import sys
import json
import os
from datetime import datetime

# --- 配置信息 ---
REDIS_HOST = "dev-core-banking-redis.lnvbm2.ng.0001.usw2.cache.amazonaws.com"       # 替换为你的 Redis 地址
REDIS_PORT = 6379
REDIS_PWD = None
REDIS_DB = 3          # 默认为 0
MATCH_PATTERN = 'load_test_key*'    # 替换为你要备份的 key 模式，如 "user:*"

# 备份批次控制
MAX_SCAN_ITERATIONS = 0        # 最多执行的 scan 批次数（0 表示不限制，直到全库扫描完毕）
START_CURSOR = 0               # 从指定 cursor 开始备份（0 表示从头开始，或自动读取上次保存的 cursor）
MAX_RECORDS_PER_FILE = 1000  # 每个备份文件最多存储的记录数，超出自动切换新文件

# 扫描与限速参数
SCAN_COUNT = 1000      # 每批扫描的 redis 键数
SLEEP_TIME = 0.1      # 每批扫描后的休眠秒数
MAX_RETRIES = 3       # 单次 scan 最大重试次数
RETRY_BACKOFF_BASE = 0.5   # 重试退避基数（秒）
RETRY_BACKOFF_CAP = 5.0    # 重试退避上限（秒）
SOCKET_TIMEOUT = 10.0      # socket 读写超时（秒）
SOCKET_CONNECT_TIMEOUT = 5.0  # 连接超时（秒）

# 备份参数
BACKUP_DIR = './redis_backups'          # 备份文件存放目录
BACKUP_BATCH_SIZE = 500                 # 每批从 Redis 获取 value 的 key 数量（用 pipeline 批量 GET）
TTL_FETCH_BATCH = 200                   # 每批获取 TTL 的 key 数量
MAX_VALUE_BYTES = 10 * 1024 * 1024      # 单个 value 超过此大小（10MB）则跳过并告警



# ============================================================
# 辅助类 / 函数
# ============================================================

class RotatingWriter:
    """支持按记录数自动分割文件的写入器"""

    def __init__(self, base_filepath, max_records):
        self.base_filepath = base_filepath
        self.max_records = max_records
        self.part = 1
        self._count = 0
        self._handle = None
        self.current_file = None
        self._open_next()

    def _open_next(self):
        if self._handle:
            self._handle.close()
        part_suffix = f'_part{self.part:03d}' if self.part > 1 else ''
        self.current_file = f'{self.base_filepath}{part_suffix}.jsonl'
        self._handle = open(self.current_file, 'w', encoding='utf-8')
        self._count = 0
        self.part += 1
        print(f"[*] 新备份文件: {self.current_file}")

    def write_record(self, record):
        self._handle.write(json.dumps(record, ensure_ascii=False) + '\n')
        self._count += 1
        if self._count >= self.max_records:
            self._open_next()

    def close(self):
        if self._handle:
            self._handle.close()
            self._handle = None
        return self.current_file

    @property
    def record_count(self):
        return self._count


def ensure_backup_dir():
    """确保备份目录存在"""
    os.makedirs(BACKUP_DIR, exist_ok=True)


def _get_cursor_file_path():
    """获取当前 DB+pattern 对应的 cursor 状态文件路径"""
    safe_pattern = MATCH_PATTERN.replace('*', 'star').replace('?', 'qmark') or 'all'
    return os.path.join(BACKUP_DIR, f'backup_db{REDIS_DB}_{safe_pattern}.cursor.json')


def _load_cursor_state():
    """加载上次备份的 cursor 状态；不存在则返回 None"""
    cursor_file = _get_cursor_file_path()
    if os.path.exists(cursor_file):
        try:
            with open(cursor_file, 'r', encoding='utf-8') as f:
                state = json.load(f)
            print(f"[*] 加载上次备份状态: cursor={state.get('cursor', 0)}, "
                  f"已扫描={state.get('scanned_count', 0)}, 已备份={state.get('backed_up_count', 0)}")
            return state
        except Exception as e:
            print(f"[!] 读取 cursor 文件失败: {e}")
    return None


def _save_cursor_state(cursor, scanned_count, backed_up_count, part):
    """保存当前备份的 cursor 状态"""
    cursor_file = _get_cursor_file_path()
    state = {
        'cursor': cursor,
        'db': REDIS_DB,
        'pattern': MATCH_PATTERN,
        'scanned_count': scanned_count,
        'backed_up_count': backed_up_count,
        'part': part,
        'timestamp': datetime.now().isoformat(),
    }
    try:
        with open(cursor_file, 'w', encoding='utf-8') as f:
            json.dump(state, f, ensure_ascii=False)
    except Exception as e:
        print(f"\n[!] 保存 cursor 状态失败: {e}")


def create_redis_client():
    """使用连接池创建 Redis 客户端，配置超时与重试策略"""
    retry = Retry(ExponentialBackoff(RETRY_BACKOFF_BASE, RETRY_BACKOFF_CAP), MAX_RETRIES)
    pool = redis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PWD,
        db=REDIS_DB,
        decode_responses=False,   # 备份场景不自动解码，保留原始字节
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
        retry=retry,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    return redis.StrictRedis(connection_pool=pool)


# ============================================================
# 主流程
# ============================================================

def backup():
    r = create_redis_client()
    ensure_backup_dir()

    # 生成带时间戳的备份文件名基础
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    safe_pattern = MATCH_PATTERN.replace('*', 'star').replace('?', 'qmark') or 'all'
    base_name = f'backup_db{REDIS_DB}_{safe_pattern}_{timestamp}'
    base_filepath = os.path.join(BACKUP_DIR, base_name)

    # --- 确定起始 cursor ---
    if START_CURSOR > 0:
        cursor = START_CURSOR
        print(f"[*] 使用配置的起始 cursor: {cursor}")
    else:
        saved_state = _load_cursor_state()
        if saved_state and saved_state.get('cursor', 0) > 0:
            cursor = saved_state['cursor']
            print(f"[*] 从上次保存的 cursor 继续: {cursor}")
        else:
            cursor = 0

    # --- 初始化 ---
    scan_count = 0
    total_scanned = 0
    total_backed_up = 0
    total_skipped_large = 0
    total_skipped_type = 0
    scan_limit = MAX_SCAN_ITERATIONS if MAX_SCAN_ITERATIONS > 0 else None

    writer = RotatingWriter(base_filepath, MAX_RECORDS_PER_FILE)
    last_cursor = cursor

    print(f"[*] 当前连接的逻辑库 (DB): {REDIS_DB}")
    print(f"[*] 扫描模式: {MATCH_PATTERN}")
    print(f"[*] SCAN_COUNT={SCAN_COUNT}, BACKUP_BATCH_SIZE={BACKUP_BATCH_SIZE}")
    print(f"[*] MAX_SCAN_ITERATIONS={MAX_SCAN_ITERATIONS if scan_limit else '不限'}, "
          f"MAX_RECORDS_PER_FILE={MAX_RECORDS_PER_FILE}")
    if cursor > 0:
        print(f"[*] 起始 cursor={cursor}")
    print("[*] 提示: 按 Ctrl+C 可以安全停止脚本并查看进度")

    try:
        while True:
            last_cursor = cursor
            cursor, keys = _scan_with_retry(r, cursor)
            scan_count += 1

            if keys:
                total_scanned += len(keys)

                # 分批获取 key 的类型、TTL 和 value，写入备份文件
                for i in range(0, len(keys), BACKUP_BATCH_SIZE):
                    batch = keys[i:i + BACKUP_BATCH_SIZE]
                    backed, skipped_large, skipped_type = _backup_batch(r, writer, batch)
                    total_backed_up += backed
                    total_skipped_large += skipped_large
                    total_skipped_type += skipped_type

                sys.stdout.write(
                    f"\r[+] DB[{REDIS_DB}] 第 {scan_count} 批 scan, "
                    f"扫描 {total_scanned} 个, 已备份 {total_backed_up} 个"
                    f"{' (跳过过大' + str(total_skipped_large) + '个)' if total_skipped_large else ''}"
                )
                sys.stdout.flush()

                # 每批 scan 后保存 cursor 状态，支持断点续传
                _save_cursor_state(cursor, total_scanned, total_backed_up, writer.part - 1)

                time.sleep(SLEEP_TIME)

            # 达到最大扫描批次数时停止
            if scan_limit and scan_count >= scan_limit:
                print(f"\n\n[!] 已达到最大扫描批次数 ({scan_limit})，停止扫描。")
                break

            if cursor == 0:
                print(f"\n\n[!] 已完成 DB[{REDIS_DB}] 的全库扫描与备份。")
                break

    except KeyboardInterrupt:
        print("\n\n[!] 检测到 Ctrl+C，正在停止脚本...")
        _save_cursor_state(last_cursor, total_scanned, total_backed_up, writer.part - 1)
    except Exception as e:
        print(f"\n[!] 运行出错: {e}")
        _save_cursor_state(last_cursor, total_scanned, total_backed_up, writer.part - 1)
    finally:
        last_file = writer.close()
        print(f"[*] 脚本运行结束。")
        print(f"    DB[{REDIS_DB}] 扫描总数:     {total_scanned}")
        print(f"    DB[{REDIS_DB}] 成功备份数:   {total_backed_up}")
        if total_skipped_large:
            print(f"    DB[{REDIS_DB}] 跳过过大value: {total_skipped_large}")
        if total_skipped_type:
            print(f"    DB[{REDIS_DB}] 跳过不支持类型: {total_skipped_type}")
        if last_file:
            print(f"    最后备份文件: {last_file}")
        print("-" * 30)


# ============================================================
# 内部辅助函数
# ============================================================

def _scan_with_retry(r, cursor):
    """带重试的 scan 操作"""
    for attempt in range(MAX_RETRIES):
        try:
            return r.scan(cursor=cursor, match=MATCH_PATTERN, count=SCAN_COUNT)
        except (TimeoutError, ConnectionError) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            print(f"\n[!] SCAN 超时/连接错误 (尝试 {attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
        except Exception:
            raise


def _backup_batch(r, writer, keys):
    """
    备份一批 key 到文件。
    writer: RotatingWriter 实例
    返回 (backed_up_count, skipped_large_count, skipped_type_count)
    """
    pipe = r.pipeline(transaction=False)
    for key in keys:
        pipe.type(key)
    types = _pipeline_execute_with_retry(r, pipe)

    # 过滤出可备份的基础类型 key
    valid_keys = []
    skipped_type = 0
    key_types = {}
    for key, key_type in zip(keys, types):
        key_type_str = key_type.decode('utf-8') if isinstance(key_type, bytes) else key_type
        if key_type_str in ('string', 'hash', 'list', 'set', 'zset', 'none'):
            valid_keys.append(key)
            key_types[key] = key_type_str
        else:
            skipped_type += 1
            print(f"\n[!] 跳过不支持的 key 类型: {key} (type={key_type_str})")

    if not valid_keys:
        return 0, 0, skipped_type

    # 批量获取 TTL
    ttl_map = {}
    for i in range(0, len(valid_keys), TTL_FETCH_BATCH):
        ttl_batch = valid_keys[i:i + TTL_FETCH_BATCH]
        pipe = r.pipeline(transaction=False)
        for key in ttl_batch:
            pipe.ttl(key)
        ttls = _pipeline_execute_with_retry(r, pipe)
        for key, ttl_val in zip(ttl_batch, ttls):
            ttl_map[key] = ttl_val if ttl_val is not None else -1

    # 批量获取 value
    pipe = r.pipeline(transaction=False)
    for key in valid_keys:
        kt = key_types[key]
        if kt == 'string' or kt == 'none':
            pipe.get(key)
        elif kt == 'hash':
            pipe.hgetall(key)
        elif kt == 'list':
            pipe.lrange(key, 0, -1)
        elif kt == 'set':
            pipe.smembers(key)
        elif kt == 'zset':
            pipe.zrange(key, 0, -1, withscores=True)
    values = _pipeline_execute_with_retry(r, pipe)

    # 写入文件（通过 RotatingWriter 自动处理分片）
    backed = 0
    skipped_large = 0
    for key, raw_value in zip(valid_keys, values):
        if raw_value is not None:
            value_size = len(raw_value) if isinstance(raw_value, bytes) else len(str(raw_value).encode('utf-8'))
            if value_size > MAX_VALUE_BYTES:
                skipped_large += 1
                print(f"\n[!] 跳过过大 value: {key} (size={value_size} bytes)")
                continue

        record = {
            'key': key.decode('utf-8') if isinstance(key, bytes) else key,
            'type': key_types[key],
            'ttl': ttl_map.get(key, -1),
            'value': _serialize_value(raw_value),
            'db': REDIS_DB,
        }
        writer.write_record(record)
        backed += 1

    return backed, skipped_large, skipped_type


def _serialize_value(raw_value):
    """
    将 Redis 返回值序列化为 JSON 兼容格式。
    bytes -> base64 编码的字符串（标记为 __binary__）
    """
    if raw_value is None:
        return None

    if isinstance(raw_value, bytes):
        # 尝试 UTF-8 解码为纯文本；失败则标记为二进制并用 base64 存储
        try:
            return raw_value.decode('utf-8')
        except (UnicodeDecodeError, ValueError):
            import base64
            return {'__binary__': True, 'data': base64.b64encode(raw_value).decode('ascii')}

    if isinstance(raw_value, dict):
        result = {}
        for k, v in raw_value.items():
            key_str = k.decode('utf-8') if isinstance(k, bytes) else k
            result[key_str] = _serialize_value(v)
        return result

    if isinstance(raw_value, list):
        return [_serialize_value(item) for item in raw_value]

    if isinstance(raw_value, set):
        return [_serialize_value(item) for item in raw_value]

    # int, float, str 等可直接序列化
    return raw_value


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
    if MATCH_PATTERN is None or MATCH_PATTERN == '':
        print(f"[*] Key 匹配模式未配置 (MATCH_PATTERN='{MATCH_PATTERN}')，请设置。")
        exit(1)

    backup()
