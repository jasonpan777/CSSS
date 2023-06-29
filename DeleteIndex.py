import sqlite3
conn = sqlite3.connect(r'stocks617.db')  # 连接到数据库
c = conn.cursor()  # 创建一个数据库游标

# 删除名为 idx_code_date 的索引
c.execute("DROP INDEX idx_code_date")

conn.commit()  # 提交数据库事务
conn.close()  # 关闭数据库连接
