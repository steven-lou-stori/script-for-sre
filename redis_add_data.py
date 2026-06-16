import logging
import random
import string
import time
import redis

# 1. 配置本地日志保存
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("sync_valkey_load_test.log"),
        logging.StreamHandler()
    ]
)


# 2. 辅助函数：生成指定大小的随机字符串
def get_random_string(length: int) -> str:
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def main():
    # --- 操作模式配置 ---
    # 您可以在此处设置操作模式："write" (写入) 或 "cleanup" (清理)
    mode = "write"  # 切换为 "cleanup" 即可清除测试数据

    # --- 连接配置 ---
    host = "dev-core-banking-redis.lnvbm2.ng.0001.usw2.cache.amazonaws.com"
    port = 6379
    REDIS_DB = 3

    try:
        logging.info(f"正在连接到 Valkey/Redis ({mode} 模式)...")
        # 创建同步客户端
        client = redis.Redis(host=host, port=port, decode_responses=True,db=REDIS_DB)

        # 测试连接是否成功
        client.ping()
        logging.info("连接成功。")

        # --- 配置参数 ---
        total_target_bytes = 3 * 1024 *1024
        value_size = 1024  # 单个 value 大小：1 KB
        total_keys = total_target_bytes // value_size
        batch_size = 500

        start_time = time.perf_counter()

        if mode == "write":
            logging.info(f"开始同步写入 3GB 测试数据，计划写入 {total_keys} 个键...")

            for start_idx in range(0, total_keys, batch_size):
                # 使用 pipeline 批量写入，提高同步执行的性能
                pipe = client.pipeline()

                for i in range(start_idx, min(start_idx + batch_size, total_keys)):
                    key = f"load_test_key:{i}"
                    value = get_random_string(value_size)
                    pipe.set(key, value)

                # 执行批处理
                pipe.execute()

                processed_keys = min(start_idx + batch_size, total_keys)
                progress = (processed_keys / total_keys) * 100
                logging.info(f"写入进度: {progress:.2f}% | 已写入 {processed_keys} / {total_keys} 个键")

        elif mode == "cleanup":
            logging.info(f"开始同步清理测试数据，计划删除 {total_keys} 个键...")

            for start_idx in range(0, total_keys, batch_size):
                pipe = client.pipeline()

                for i in range(start_idx, min(start_idx + batch_size, total_keys)):
                    key = f"load_test_key_{i}"
                    pipe.delete(key)

                # 执行批处理
                pipe.execute()

                processed_keys = min(start_idx + batch_size, total_keys)
                progress = (processed_keys / total_keys) * 100
                logging.info(f"清理进度: {progress:.2f}% | 已清理 {processed_keys} / {total_keys} 个键")

        elapsed = time.perf_counter() - start_time
        logging.info(f"操作完成！总耗时: {elapsed:.2f} 秒。")

    except redis.exceptions.ConnectionError as e:
        logging.critical(f"连接失败: {e}")
    except Exception as e:
        logging.critical(f"执行异常: {e}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        logging.info("任务已由用户停止。")