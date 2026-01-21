#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
GBase8c主备高可用读取测试脚本
功能说明：
1. 无限循环读取（仅手动终止），主备倒换时自动重试无阻塞
2. 适配集群IP：172.x.x.231/232，浮动IP 230
"""

import psycopg2
import os
import time
from dotenv import load_dotenv

# ======================== 统一格式报告生成函数 =========================
def generate_ha_test_report(test_type, duration, reconnect_num, 
                           exclusive_metrics):
    """
    生成统一格式的高可用测试报告
    :param test_type: 测试类型（数据写入/数据读取）
    :param duration: 测试时长（秒）
    :param reconnect_num: 重连次数
    :param exclusive_metrics: 专属指标字典（key=字段名，value=值）
    :return: 格式化报告字符串
    """
    # 拼接专属指标字段
    exclusive_str = ""
    for key, value in exclusive_metrics.items():
        exclusive_str += f"{key:<8}： {value}\n"
    
    # 生成报告
    report = f"""========================================
        GBase8c主备式高可用-读取测试
========================================\n
测试类型    ： {test_type}
测试时长    ： {duration:.2f} 秒
重连次数    ： {reconnect_num} 次
\n----------------------------------------\n
{exclusive_str}
========================================
"""
    return report

# ======================== 配置加载与参数定义 =========================
load_dotenv()

# 核心测试参数（与写入脚本完全对齐）
TEST_CONFIG = {
    "normal_interval": 0.1,       # 正常读取间隔（秒），与写入间隔一致
    "retry_interval": 2,          # 故障重试间隔（秒），与写入间隔一致
    "db_retry_times": 2,          # 单次重连重试次数，与写入间隔一致
    "read_timeout": 2,            # 单次读取超时时间（秒），对应写入超时
    "connect_timeout": 3,         # 数据库连接超时（秒），与写入连接超时一致
}

# 数据库连接配置（浮动IP 172.x.x.230）
DB_CONFIG = {
    'database': os.getenv('GB_DB', 'postgres'),
    'user': os.getenv('GB_USER', 'testuser'),
    'password': os.getenv('GB_PWD', ''),
    'host': os.getenv('GB_HOST', '172.x.x.230'),  # 建议用VIP/代理地址
    'port': int(os.getenv('GB_PORT', 15400)),
    'connect_timeout': TEST_CONFIG["connect_timeout"],
    'keepalives': 1,
    'keepalives_idle': 10,
    'keepalives_interval': 5,
    'keepalives_count': 3,
    'sslmode': 'disable',
}

# 集群节点IP配置（用于角色描述）
CLUSTER_NODES = {
    "172.x.x.231": "231",
    "172.x.x.232": "232"
}

# 统计指标（高可用测试核心）
stats = {
    'total_reads': 0,
    'success_reads': 0,
    'failed_reads': 0,
    'reconnect_times': 0,  # 仅统计非首次的重连次数
    'start_time': None,
    'end_time': None
}

# ======================== 核心工具函数 =========================
def get_master_ip(standby_ip):
    """
    根据备库IP自动推导主库IP（从CLUSTER_NODES中匹配）
    :param standby_ip: 备库IP地址
    :return: 主库IP的精简描述（如"232(172.x.x.232)"）
    """
    if not standby_ip or standby_ip == "无" or standby_ip == "未知":
        return "未知"

    # 从集群节点中排除备库IP，剩余的即为主库IP
    cluster_ips = list(CLUSTER_NODES.keys())
    if standby_ip in cluster_ips:
        master_ips = [ip for ip in cluster_ips if ip != standby_ip]
        if master_ips:
            master_ip = master_ips[0]
            return f"{CLUSTER_NODES[master_ip]}({master_ip})"

    # 未匹配到的情况
    return f"未知({standby_ip})"

def get_replication_info(conn):
    """
    获取复制状态信息：角色描述、备库连接数、备库IP、同步状态
    返回格式：(节点角色描述, 备库连接数, 备库IP, 同步状态)
    优化：移除角色描述中的(无备)/(备1)等后缀
    """
    try:
        cur = conn.cursor()

        # 1. 核心判断1：恢复状态（主库=非恢复，备库=恢复中）
        cur.execute("SELECT pg_is_in_recovery();")
        is_standby = cur.fetchone()[0]

        # 2. 核心判断2：备库连接数
        cur.execute("SELECT count(*) FROM pg_stat_replication;")
        repl_count = cur.fetchone()[0]

        # 3. 获取备库详细信息（IP+同步状态）
        standby_ip = "无"
        sync_state = "无"
        master_ip_desc = "未知"
        if repl_count > 0:
            cur.execute("SELECT client_addr, state FROM pg_stat_replication LIMIT 1;")
            result = cur.fetchone()
            if result:
                standby_ip = result[0] if result[0] else "未知"
                sync_state = result[1] if result[1] else "未知"
                # 自动推导主库IP
                master_ip_desc = get_master_ip(standby_ip)

        # 4. 组合判断节点角色（精简描述，移除(无备)/(备1)后缀）
        if not is_standby:  # 非恢复状态 = 主库
            # 只保留主库+IP，移除(备1)后缀
            node_role = f"主库{master_ip_desc}" if master_ip_desc != "未知" else "主库"
        else:  # 恢复状态 = 备库（只读）
            node_role = "备库"

        # 5. 补充备库IP的精简描述
        if standby_ip in CLUSTER_NODES:
            standby_ip_desc = f"{CLUSTER_NODES[standby_ip]}({standby_ip})"
        else:
            standby_ip_desc = standby_ip

        cur.close()
        return node_role, repl_count, standby_ip_desc, sync_state

    except Exception as e:
        err_tag = str(e)[:15].replace(" ", "_")
        return f"角色获取失败_{err_tag}", -1, "获取失败", "获取失败"

def check_ha_status(conn):
    """检测当前连接节点是否为主库+获取节点IP"""
    if not conn:
        return False, "unknown"
    try:
        cur = conn.cursor()
        # 1. 主备角色检测
        cur.execute("SELECT pg_is_in_recovery();")
        is_standby = cur.fetchone()[0]
        # 2. 获取当前节点IP
        cur.execute("SELECT inet_server_addr();")
        server_ip = cur.fetchone()[0] or "localhost"
        cur.close()
        return not is_standby, server_ip  # (是否主库, 节点IP)
    except Exception as e:
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 主备状态检测失败：{str(e)[:60]}")
        return False, "unknown"

def get_conn(is_first_connect):
    """
    获取有效数据库连接，失败时重试（参数与写入脚本对齐）
    :param is_first_connect: 是否是首次连接（用于控制重连次数统计）
    """
    for i in range(TEST_CONFIG["db_retry_times"]):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = True
            # 验证连接有效性
            cur = conn.cursor()
            cur.execute("select 1")
            cur.close()
            # 验证主备状态
            is_primary, server_ip = check_ha_status(conn)
            if is_primary:
                # 仅非首次连接时统计重连次数
                if not is_first_connect:
                    stats['reconnect_times'] += 1
                return conn, server_ip
            else:
                conn.close()
                print(f"  连接重试失败：当前节点({server_ip})是备库，第{i+1}次重试")
        except Exception as e:
            err = str(e)[:60]
            print(f"  连接重试失败：{err}")
            # 重试间隔与写入脚本一致（0.5秒）
            time.sleep(0.5)
    return None, "unknown"

def check_conn(conn):
    """检查连接是否存活"""
    if not conn or conn.closed != 0:
        return False
    try:
        conn.cursor().execute("select 1")
        return True
    except:
        return False

def read_first_row(conn):
    """读取test_table表中最新数据，返回最新序号"""
    try:
        # 获取复制状态信息
        node_role, repl_count, standby_ip, sync_state = get_replication_info(conn)

        cur = conn.cursor()
        # 读取最新的一条数据（按insert_seq降序）
        cur.execute("SELECT insert_seq FROM test_table ORDER BY insert_seq DESC LIMIT 1;")
        row = cur.fetchone()
        cur.close()

        if row:
            # 仅返回最新序号
            latest_seq = row[0]
            data_desc = f"{latest_seq}"
        else:
            data_desc = "无数据"
            
        return True, data_desc, node_role, standby_ip, sync_state
    except Exception as e:
        return False, str(e)[:80], "未知", "未知", "未知"

# ======================== 核心测试逻辑 =========================
def main():
    """主测试流程：无限循环读取"""
    stats['start_time'] = time.time()
    conn = None
    current_node_ip = "unknown"
    is_first_connect = True  # 新增：首次连接标记

    # 打印测试配置（与写入脚本格式对齐）
    print(f"\n启动GBase8c高可用读取测试（浮动IP：{DB_CONFIG['host']}）")
    print(f"========================================")
    print(f"正常读取间隔：{TEST_CONFIG['normal_interval']}秒/次")
    print(f"故障重试间隔：{TEST_CONFIG['retry_interval']}秒")
    print(f"单次重连重试：{TEST_CONFIG['db_retry_times']}次")
    print(f"读取超时时间：{TEST_CONFIG['read_timeout']}秒")
    print(f"连接超时时间：{TEST_CONFIG['connect_timeout']}秒")
    print(f"集群节点：{CLUSTER_NODES}")
    print(f"========================================")
    print(f"开始无限读取（按Ctrl+C终止）...\n")

    # 无限循环读取
    try:
        while True:
            try:
                # 1. 连接有效性检查（失效则重连）
                if not check_conn(conn):
                    if is_first_connect:
                        # 首次连接：友好提示
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 首次建立数据库连接...")
                    else:
                        # 非首次：原重连提示
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 连接失效，重连数据库...")
                    
                    if conn and not is_first_connect:
                        conn.close()
                    
                    # 重新获取连接（传入首次连接标记）
                    conn, current_node_ip = get_conn(is_first_connect)
                    if not conn:
                        err_msg = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 连接失败，{TEST_CONFIG['retry_interval']}秒后重试"
                        print(err_msg)
                        stats['failed_reads'] += 1
                        time.sleep(TEST_CONFIG["retry_interval"])
                        continue
                    
                    # 获取连接后的角色信息
                    is_primary, _ = check_ha_status(conn)
                    role_desc = "主库" if is_primary else "备库"
                    
                    # 区分首次连接和重连的提示文案
                    if is_first_connect:
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 首次连接成功，当前节点={current_node_ip}（{role_desc}）")
                        is_first_connect = False  # 首次连接后标记为False
                    else:
                        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 重连成功，当前节点={current_node_ip}（{role_desc}）")

                # 2. 执行读取操作
                stats['total_reads'] += 1
                success, latest_seq, node_role, standby_ip, sync_state = read_first_row(conn)

                if success:
                    stats['success_reads'] += 1
                    # 调整输出格式：读取行序号移到成功后，修改文案
                    success_msg = (
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 成功 | 读取行序号{latest_seq} | 角色：{node_role} | 备库：{standby_ip}"
                    )
                    print(success_msg)
                else:
                    stats['failed_reads'] += 1
                    # 调整失败输出格式，保持字段顺序一致
                    fail_msg = (
                        f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 失败 | 读取行序号- | 角色：{node_role} | 备库：{standby_ip} | 原因：{latest_seq}"
                    )
                    print(fail_msg)

                # 3. 按业务级间隔休眠（0.1秒/次，与写入频率一致）
                time.sleep(TEST_CONFIG["normal_interval"])

            except Exception as e:
                # 全局异常处理（统一重试间隔）
                conn = None
                err = str(e)[:80]
                stats['failed_reads'] += 1
                print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] 异常 | 读取行序号- | 角色：未知 | 备库：未知 | 原因：{err}，{TEST_CONFIG['retry_interval']}秒后重试")
                time.sleep(TEST_CONFIG["retry_interval"])

    except KeyboardInterrupt:
        # 手动终止时输出统一格式报告
        stats['end_time'] = time.time()
        actual_duration = stats['end_time'] - stats['start_time']
        success_rate = (stats['success_reads'] / stats['total_reads']) * 100 if stats['total_reads'] > 0 else 0
        avg_read_speed = round(stats['total_reads'] / actual_duration, 2) if actual_duration > 0 else 0

        # 构造读取测试专属指标
        read_exclusive = {
            "总读取次数": f"{stats['total_reads']} 次",
            "成功次数": f"{stats['success_reads']} 次 | 失败次数：{stats['failed_reads']} 次",
            "读取成功率": f"{success_rate:.2f}%",
            "平均读取速度": f"{avg_read_speed} 次/秒"
        }

        # 生成统一格式报告
        report = generate_ha_test_report(
            test_type="数据读取",
            duration=actual_duration,
            reconnect_num=stats['reconnect_times'],
            exclusive_metrics=read_exclusive
            )

        # 打印报告
        print(f"\n{report}")

        # 关闭连接
        if conn:
            conn.close()

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"\n测试异常终止：{str(e)[:100]}")
