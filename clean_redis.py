import redis
from redis.retry import Retry
from redis.backoff import ExponentialBackoff
from redis.exceptions import TimeoutError, ConnectionError
import time
import sys

# --- 配置信息 ---
REDIS_HOST = 'dev-core-banking-redis.lnvbm2.ng.0001.usw2.cache.amazonaws.com'       # 替换为你的 Redis 地址
REDIS_PORT = 6379
REDIS_PWD = None
REDIS_DB = 3          # 默认为 0，可为 None 表示不设置
MATCH_PATTERN = 'load_test_key*'    # 替换为你要匹配的 key 模式，如 "user:*"

# 清理批次控制
MAX_SCAN_ITERATIONS = 0  # 最多执行的 scan 批次数（0 表示不限制，直到全库扫描完毕）

# 扫描与限速参数
SCAN_COUNT = 1000      # 每批扫描的redis键数
SLEEP_TIME = 0.1      # 每批扫描后的休眠秒数
MAX_RETRIES = 3       # 单次 scan/unlink 最大重试次数
RETRY_BACKOFF_BASE = 0.5   # 重试退避基数（秒）
RETRY_BACKOFF_CAP = 5.0    # 重试退避上限（秒）
SOCKET_TIMEOUT = 10.0      # socket 读写超时（秒）
SOCKET_CONNECT_TIMEOUT = 5.0  # 连接超时（秒）


def create_redis_client():
    """使用连接池创建 Redis 客户端，配置超时与重试策略"""
    retry = Retry(ExponentialBackoff(RETRY_BACKOFF_BASE, RETRY_BACKOFF_CAP), MAX_RETRIES)
    pool = redis.ConnectionPool(
        host=REDIS_HOST,
        port=REDIS_PORT,
        password=REDIS_PWD,
        db=REDIS_DB,
        decode_responses=True,
        socket_timeout=SOCKET_TIMEOUT,
        socket_connect_timeout=SOCKET_CONNECT_TIMEOUT,
        retry=retry,
        retry_on_timeout=True,
        health_check_interval=30,
    )
    return redis.StrictRedis(connection_pool=pool)


def batch_delete():
    r = create_redis_client()

    cursor = 0
    scan_count = 0        # 已执行的 scan 批次数
    total_attempted = 0    # scan 出来的总 key 数
    total_actually_deleted = 0  # unlink 实际返回的删除数

    scan_limit = MAX_SCAN_ITERATIONS if MAX_SCAN_ITERATIONS > 0 else None

    print(f"[*] 当前连接的逻辑库 (DB): {REDIS_DB}")
    print(f"[*] 开始扫描模式: {MATCH_PATTERN}")
    print(f"[*] SCAN_COUNT={SCAN_COUNT}, MAX_SCAN_ITERATIONS={MAX_SCAN_ITERATIONS if scan_limit else '不限'}, SLEEP_TIME={SLEEP_TIME}s")
    print("[*] 提示: 按 Ctrl+C 可以安全停止脚本并查看进度")

    try:
        while True:
            # 带重试的 scan
            cursor, keys = _scan_with_retry(r, cursor)
            scan_count += 1

            if keys:
                total_attempted += len(keys)
                deleted_count = _unlink_with_retry(r, keys)
                total_actually_deleted += deleted_count

                sys.stdout.write(
                    f"\r[+] DB[{REDIS_DB}] 第 {scan_count} 批扫描, 扫描 {total_attempted} 个, 实际删除 {total_actually_deleted} 个"
                )
                sys.stdout.flush()
                time.sleep(SLEEP_TIME)

            # 达到最大扫描批次数时停止
            if scan_limit and scan_count >= scan_limit:
                print(f"\n\n[!] 已达到最大扫描批次数 ({scan_limit})，停止扫描。")
                break

            if cursor == 0:
                print(f"\n\n[!] 已完成 DB[{REDIS_DB}] 的全库扫描。")
                break

    except KeyboardInterrupt:
        print("\n\n[!] 检测到 Ctrl+C，正在停止脚本...")
    except Exception as e:
        print(f"\n[!] 运行出错: {e}")
    finally:
        print(f"[*] 脚本运行结束。")
        print(f"    DB[{REDIS_DB}] 扫描总数: {total_attempted}")
        print(f"    DB[{REDIS_DB}] 实际删除数: {total_actually_deleted}")
        print("-" * 30)


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


def _unlink_with_retry(r, keys):
    """带重试的 unlink 操作，返回实际删除数"""
    for attempt in range(MAX_RETRIES):
        try:
            return r.unlink(*keys)
        except (TimeoutError, ConnectionError) as e:
            if attempt == MAX_RETRIES - 1:
                raise
            print(f"\n[!] UNLINK 超时/连接错误 (尝试 {attempt+1}/{MAX_RETRIES}): {e}")
            time.sleep(RETRY_BACKOFF_BASE * (2 ** attempt))
        except Exception:
            raise


if __name__ == "__main__":
    # 修复：使用 is None 判空，避免 not 0 把合法的 DB=0 误判为空
    if not REDIS_HOST:
        print(f"[*] Redis 地址未配置 (REDIS_HOST='{REDIS_HOST}')，请设置。")
        exit(1)
    if MATCH_PATTERN is None or MATCH_PATTERN == '':
        print(f"[*] Key 匹配模式未配置 (MATCH_PATTERN='{MATCH_PATTERN}')，请设置。")
        exit(1)
    # REDIS_DB=0 是合法值，不再校验"是否为空"（默认就是 0）

    batch_delete()