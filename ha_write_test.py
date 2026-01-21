#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GBase8c主备高可用写入测试脚本
核心功能：
1. 测试前置：自动清空test_table表数据
2. 高可用写入：无限循环写入，主备倒换时自动重试无阻塞
3. 节点感知：精准识别主备角色，自动推导主库IP地址
4. 超时控制：线程级插入超时，避免单点故障导致脚本阻塞
5. 统一输出：标准化日志格式，支持手动终止生成测试报告
6. 环境适配：支持.env配置文件，适配172.x.x.230/231/232集群环境

适用场景：
- GBase8c主备架构高可用验证
- 主备倒换场景下的数据写入连续性测试
- 集群故障自动恢复能力验证

自己想功能逻辑，剩下的交给豆包
熟练 Ctrl C + Ctrl V
就这三行字100%出自手敲键盘
"""

import psycopg2
import os
import time
import threading
from dotenv import load_dotenv
from typing import Dict, Tuple, Optional, Any

# ======================== 全局配置与常量定义 =========================
# 加载环境变量（优先读取.env文件，无则使用默认值）
load_dotenv()

# 核心测试参数配置（可根据测试场景调整）
TEST_CONFIG: Dict[str, float] = {
    "normal_interval": 0.1,       # 正常写入间隔（秒/条）
    "retry_interval": 2.0,        # 故障重试间隔（秒）
    "db_retry_times": 2,          # 单次重连重试次数
    "insert_timeout": 2.0,        # 单次插入超时时间（秒）
    "connect_timeout": 3.0,       # 数据库连接超时（秒）
}

# 数据库连接配置（浮动IP：172.x.x.230）
DB_CONFIG: Dict[str, Any] = {
    'database': os.getenv('GB_DB', 'postgres'),
    'user': os.getenv('GB_USER', 'test01'),
    'password': os.getenv('GB_PWD', ''),
    'host': os.getenv('GB_HOST', '172.x.x.230'),
    'port': int(os.getenv('GB_PORT', 15400)),
    'connect_timeout': TEST_CONFIG["connect_timeout"],
    'options': '-c synchronous_commit=off',  # 强制异步提交提升写入性能
}

# 集群节点映射（用于简化IP展示）
CLUSTER_NODES: Dict[str, str] = {
    "172.x.x.231": "231",
    "172.x.x.232": "232"
}

# 全局统计指标
STATS: Dict[str, int] = {
    "reconnect_count": 0,         # 数据库重连次数
    "total_inserts": 0,           # 累计插入次数
    "success_inserts": 0,         # 成功插入次数
    "failed_inserts": 0           # 失败插入次数
}

# ======================== 报告生成工具函数 =========================
def generate_ha_report(
    test_type: str,
    duration: float,
    reconnect_num: int,
    last_node: str,
    exclusive_metrics: Dict[str, str]
    ) -> str:
    """
    生成标准化高可用测试报告
    
    Args:
        test_type: 测试类型（如"数据写入"）
        duration: 测试时长（秒）
        reconnect_num: 重连次数
        last_node: 最后连接节点信息
        exclusive_metrics: 测试专属指标字典
    
    Returns:
        格式化的测试报告字符串
    """
    # 拼接专属指标
    exclusive_str = ""
    for key, value in exclusive_metrics.items():
        exclusive_str += f"{key:<10}： {value}\n"
    
    # 生成结构化报告
    report = f"""========================================
        GBase8c主备式高可用-写入测试
========================================\n
测试类型        ： {test_type}
测试时长        ： {duration:.2f} 秒
重连次数        ： {reconnect_num} 次
最后连接节点    ： {last_node}
\n----------------------------------------\n
{exclusive_str}
========================================
"""
    return report

# ======================== 数据库操作工具函数 =========================
def clear_test_table() -> None:
    """
    测试前置操作：清空test_table表数据
    异常处理：清空失败时抛出异常，终止测试（避免脏数据影响）
    """
    print(f"\n[前置操作] 清空test_table表数据...")
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("TRUNCATE TABLE test_table;")
        conn.close()
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] test_table表清空成功")
    except Exception as e:
        error_msg = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 表清空失败：{str(e)[:80]}"
        print(error_msg)
        raise RuntimeError(error_msg)

def get_master_ip_from_standby(standby_ip: str) -> str:
    """
    根据备库IP推导主库IP（从集群节点列表中匹配）
    
    Args:
        standby_ip: 备库IP地址
    
    Returns:
        主库IP精简描述（如"231(172.x.x.231)"）
    """
    if not standby_ip or standby_ip in ("无", "未知"):
        return "未知"
    
    cluster_ips = list(CLUSTER_NODES.keys())
    if standby_ip in cluster_ips:
        master_ips = [ip for ip in cluster_ips if ip != standby_ip]
        if master_ips:
            master_ip = master_ips[0]
            return f"{CLUSTER_NODES[master_ip]}({master_ip})"
    
    return f"未知({standby_ip})"

def get_replication_status(conn: psycopg2.extensions.connection) -> Tuple[str, int, str, str]:
    """
    获取数据库节点复制状态信息
    
    Args:
        conn: 数据库连接对象
    
    Returns:
        元组(节点角色描述, 备库连接数, 备库IP描述, 同步状态)
    """
    try:
        with conn.cursor() as cur:
            # 1. 判断节点是否为备库（恢复中状态）
            cur.execute("SELECT pg_is_in_recovery();")
            is_standby = cur.fetchone()[0]
            
            # 2. 获取备库连接数
            cur.execute("SELECT count(*) FROM pg_stat_replication;")
            repl_count = cur.fetchone()[0]
            
            # 3. 获取备库详细信息
            standby_ip = "无"
            sync_state = "无"
            master_ip_desc = "未知"
            
            if repl_count > 0:
                cur.execute("SELECT client_addr, state FROM pg_stat_replication LIMIT 1;")
                result = cur.fetchone()
                if result:
                    standby_ip = result[0] or "未知"
                    sync_state = result[1] or "未知"
                    master_ip_desc = get_master_ip_from_standby(standby_ip)
            
            # 4. 生成节点角色描述（精简版）
            if not is_standby:
                node_role = f"主库{master_ip_desc}" if master_ip_desc != "未知" else "主库"
            else:
                node_role = "备库"
            
            # 5. 格式化备库IP展示
            if standby_ip in CLUSTER_NODES:
                standby_ip_desc = f"{CLUSTER_NODES[standby_ip]}({standby_ip})"
            else:
                standby_ip_desc = standby_ip if standby_ip != "无" else "无"
        
        return node_role, repl_count, standby_ip_desc, sync_state
    
    except Exception as e:
        err_tag = str(e)[:15].replace(" ", "_")
        return f"角色获取失败_{err_tag}", -1, "获取失败", "获取失败"

def create_db_connection() -> Optional[psycopg2.extensions.connection]:
    """
    创建数据库短连接（带重试机制）
    
    Returns:
        成功返回连接对象，失败返回None
    """
    global STATS
    for retry in range(int(TEST_CONFIG["db_retry_times"])):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            # 验证连接有效性
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
            return conn
        except Exception as e:
            err_msg = str(e)[:60]
            print(f"  [重连重试 {retry+1}/{TEST_CONFIG['db_retry_times']}] 失败：{err_msg}")
            time.sleep(0.5)
    
    # 重连失败，更新统计
    STATS["reconnect_count"] += 1
    return None

# ======================== 写入操作核心函数 =========================
def insert_data_with_timeout(
    conn: psycopg2.extensions.connection,
    seq: int,
    timeout: float
) -> Dict[str, Any]:
    """
    带超时控制的插入操作（线程级）
    
    Args:
        conn: 数据库连接对象
        seq: 插入序号
        timeout: 超时时间（秒）
    
    Returns:
        操作结果字典，包含success/error/node_role等字段
    """
    result: Dict[str, Any] = {
        "success": False,
        "error": "",
        "conn": conn,
        "node_role": "",
        "standby_ip": "",
        "sync_state": ""
    }

    def insert_task() -> None:
        """插入任务子线程"""
        nonlocal result
        try:
            # 获取节点复制状态
            node_role, _, standby_ip, sync_state = get_replication_status(conn)
            result.update({
                "node_role": node_role,
                "standby_ip": standby_ip,
                "sync_state": sync_state
            })
            
            # 执行插入操作（强制异步提交）
            with conn.cursor() as cur:
                cur.execute("SET synchronous_commit = off;")
                cur.execute(
                    "INSERT INTO test_table (insert_seq, insert_time) VALUES (%s, now());",
                    (seq,)
                )
            result["success"] = True
        
        except Exception as e:
            result["error"] = str(e)[:80]
            result["conn"] = None
            # 失败时关闭连接
            if conn:
                try:
                    conn.close()
                except:
                    pass

    # 启动线程执行插入
    insert_thread = threading.Thread(target=insert_task)
    insert_thread.daemon = True
    insert_thread.start()
    insert_thread.join(timeout=timeout)

    # 处理超时场景
    if insert_thread.is_alive():
        result["error"] = f"插入超时（{timeout}秒），节点可能发生主备倒换或宕机"
        result["conn"] = None
        try:
            conn.close()
        except:
            pass
    
    return result

# ======================== 主测试流程 =========================
def main() -> None:
    """
    主测试流程：无限循环写入数据，支持主备倒换自动重试
    手动按Ctrl+C终止测试并生成报告
    """
    insert_seq = 1
    start_time = time.time()
    last_node_info = "未知"
    
    # 执行前置操作
    clear_test_table()

    # 打印测试配置信息
    print(f"\n[测试启动] GBase8c主备高可用写入测试（浮动IP：{DB_CONFIG['host']}）")
    print(f"========================================")
    print(f"正常写入间隔：{TEST_CONFIG['normal_interval']}秒/条")
    print(f"故障重试间隔：{TEST_CONFIG['retry_interval']}秒")
    print(f"单次插入超时：{TEST_CONFIG['insert_timeout']}秒")
    print(f"数据库重连次数：{TEST_CONFIG['db_retry_times']}次")
    print(f"集群节点：{CLUSTER_NODES}")
    print(f"========================================")
    print(f"测试开始（按Ctrl+C终止）...\n")

    try:
        while True:
            conn = None
            try:
                # 1. 创建数据库连接
                conn = create_db_connection()
                if not conn:
                    err_msg = (
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 数据库连接失败，"
                        f"{TEST_CONFIG['retry_interval']}秒后重试"
                    )
                    print(err_msg)
                    STATS["failed_inserts"] += 1
                    time.sleep(TEST_CONFIG["retry_interval"])
                    continue

                # 2. 执行带超时的插入操作
                insert_result = insert_data_with_timeout(
                    conn=conn,
                    seq=insert_seq,
                    timeout=TEST_CONFIG["insert_timeout"]
                )

                # 更新最后节点信息
                last_node_info = insert_result.get("node_role", "未知")

                # 3. 处理插入结果
                if insert_result["success"]:
                    STATS["success_inserts"] += 1
                    STATS["total_inserts"] += 1
                    # 成功日志格式：时间 | 结果 | 序号 | 角色 | 备库 | 状态
                    success_msg = (
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 成功 | 插入行序号{insert_seq} | "
                        f"角色：{insert_result['node_role']} | 备库：{insert_result['standby_ip']} | 状态：{insert_result['sync_state']}"
                    )
                    print(success_msg)
                    insert_seq += 1
                    time.sleep(TEST_CONFIG["normal_interval"])
                else:
                    STATS["failed_inserts"] += 1
                    STATS["total_inserts"] += 1
                    # 失败日志格式
                    fail_msg = (
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 失败 | 插入行序号{insert_seq} | 原因：{insert_result['error']}"
                    )
                    print(fail_msg)
                    time.sleep(TEST_CONFIG["retry_interval"])

            except Exception as e:
                # 全局异常捕获
                STATS["failed_inserts"] += 1
                STATS["total_inserts"] += 1
                err_msg = str(e)[:80]
                exception_msg = (
                    f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 异常 | 插入行序号{insert_seq} | "
                    f"原因：{err_msg}，{TEST_CONFIG['retry_interval']}秒后重试"
                )
                print(exception_msg)
                time.sleep(TEST_CONFIG["retry_interval"])
            finally:
                # 确保连接关闭
                if conn and insert_result.get("conn") is not None:
                    try:
                        conn.close()
                    except:
                        pass

    except KeyboardInterrupt:
        # 手动终止测试，生成报告
        end_time = time.time()
        total_duration = end_time - start_time
        avg_speed = round(STATS["success_inserts"] / total_duration, 2) if total_duration > 0 else 0
        success_rate = round(
            (STATS["success_inserts"] / STATS["total_inserts"]) * 100, 2
        ) if STATS["total_inserts"] > 0 else 0

        # 构造专属指标
        write_metrics = {
            "总插入次数": f"{STATS['total_inserts']} 次",
            "成功插入次数": f"{STATS['success_inserts']} 次",
            "失败插入次数": f"{STATS['failed_inserts']} 次",
            "插入成功率": f"{success_rate}%",
            "平均写入速度": f"{avg_speed} 条/秒",
            "最后同步备库": (
                f"{insert_result.get('standby_ip', '未知')}（状态：{insert_result.get('sync_state', '未知')}）"
                if 'insert_result' in locals() else "未知（状态：未知）"
            )
        }

        # 生成并打印报告
        report = generate_ha_report(
            test_type="数据写入",
            duration=total_duration,
            reconnect_num=STATS["reconnect_count"],
            last_node=last_node_info,
            exclusive_metrics=write_metrics)
        print(f"\n{report}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n[致命错误] 测试异常终止：{str(e)[:100]}")
        exit(1)