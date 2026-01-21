
# GBase8c 高可用测试脚本-学习实验用
适用于GBase8c主备架构的高可用测试，包含数据写入/读取高可用测试、表创建查询等脚本。

## 功能说明
1. `ha_write_test.py`：主备倒换场景下的无限循环写入测试
3. `ha_read_test.py`：主备倒换场景下的无限循环读取测试

## 使用前提
1. 安装依赖：`pip install psycopg2  python-dotenv`

2. 配置数据库连接：修改脚本中的DB_CONFIG或创建.env文件

3. .env文件格式

   ```
   GB_HOST=172.x.x.230
   GB_PORT=15400
   GB_USER=testuser
   GB_PWD=xxxxxxxxxxxxxxx
   GB_DB=postgres
   ```

## 运行方式
```bash

# 写入测试脚本
python3 ha_write_test.py

# 读取测试脚本
python3 ha_read_test.py

```

