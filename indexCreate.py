import sqlite3

#创建索引函数，用于提高
conn = sqlite3.connect(r'stocks617.db')  # 连接到数据库
c = conn.cursor()  # 创建一个数据库游标

# 在 historical_data 表的 CODE 和 date 字段上创建一个索引，命名为 idx_code_date
c.execute("CREATE INDEX IF NOT EXISTS idx_code_date ON historical_data (CODE, date)")

conn.commit()  # 提交数据库事务
conn.close()  # 关闭数据库连接
