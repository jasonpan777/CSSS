import sqlite3
import pandas as pd
import os
import importlib.util
from multiprocessing import Pool
import multiprocessing as mp
import datetime
from tqdm import tqdm

# 定义获取单个股票数据的函数
def fetch_stock_data(stock_code):
    # 连接数据库，采用绝对路径
    conn = sqlite3.connect(r'stocks617.db')#数据库
    # 读取特定股票的历史数据（最近的300条数据）这样能够保证所有的数据都能被选取到
    df = pd.read_sql_query(f"SELECT * FROM historical_data WHERE CODE='{stock_code}' ORDER BY date DESC LIMIT 300", conn)
    conn.close()  # 关闭数据库连接

    # 检查数据是否为空
    if df.empty:
        return None  # 如果数据为空，返回 None

    df = df.sort_values('date')  # 按日期升序排序，这一步很重要
    return df  # 返回获取的数据

# 定义运行选股策略的函数
def run_strategies(stock_code):
    df = fetch_stock_data(stock_code)  # 获取特定股票的数据

    # 检查数据是否为空
    if df is None:
        return None  # 如果数据为空，返回 None，跳过后续的处理

    strategies_path = "StrV2"  # 定义策略的路径
    result = pd.DataFrame()  # 创建一个空的数据框来存储结果

    # 循环读取并运行每一个策略
    for filename in os.listdir(strategies_path):
        if filename.endswith(".py"):  # 确保文件是Python文件
            try:
                # 导入策略
                spec = importlib.util.spec_from_file_location(
                    name=filename[:-3],
                    location=os.path.join(strategies_path, filename)
                )
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)

                # 运行策略并获取结果
                strategy_result = mod.run_strategy(df).copy()

                # 如果策略有结果，则添加到总的结果中
                if strategy_result is not None and not strategy_result.empty:
                    strategy_result["strategy"] = filename[:-3]
                    result = pd.concat([result, strategy_result], ignore_index=True)

            except Exception as e:
                print(f"An error occurred while running the strategy '{filename[:-3]}': {e}")

    # 对结果进行处理：按照日期和股票代码分组，计算策略数量，然后排序
    if not result.empty:
        result = result.groupby(['date', 'CODE']).agg(
            {'strategy': lambda x: list(x), 'O': 'first', 'L': 'first', 'H': 'first', 'C': 'first',
             'VOL': 'first'}).reset_index()
        result['strategy_count'] = result['strategy'].apply(len)
    return result  # 返回结果

# 定义获取所有股票代码的函数
def get_all_stock_codes():
    conn = sqlite3.connect(r'stocks617.db')  # 连接数据库
    # 从数据库中获取所有独特的股票代码
    stock_codes = pd.read_sql_query("SELECT DISTINCT CODE FROM historical_data", conn)
    conn.close()  # 关闭数据库连接
    return stock_codes['CODE'].tolist()  # 返回股票代码列表

# 定义将结果写入CSV文件的函数
def write_to_csv(df, filename):
    # 按照策略数量和日期排序
    df.sort_values(by=['strategy_count', 'date'], ascending=[False, False], inplace=True)
    df.to_csv(filename, mode='a', header=False, index=False)  # 写入CSV文件

# 定义进度条更新的函数
def update_progress_bar(*a):
    pbar.update()

# 定义运行所有策略的函数
def run_all_strategies(stock_codes):
    results = []  # 初始化结果列表
    all_results = pd.DataFrame()  # 创建一个空的数据框来收集所有结果

    # 使用多进程运行所有的策略
    with Pool(processes=mp.cpu_count()) as pool:
        for stock_code in stock_codes:
            result = pool.apply_async(run_strategies, args=(stock_code,), callback=update_progress_bar)
            results.append(result)  # 将结果添加到结果列表中
        pool.close()
        pool.join()

    # 获取每个进程的结果，如果结果不为 None，添加到总的结果中
    for result in results:
        res = result.get()
        if res is not None:  # 如果结果不为空
            all_results = pd.concat([all_results, res], ignore_index=True)  # 添加结果到总的结果中

    pbar.close()

    # 对所有结果进行排序，然后写入CSV文件
    all_results.sort_values(by=['strategy_count', 'date'], ascending=[False, False], inplace=True)

    # 获取当前日期并转换为字符串格式，然后用它来构造新的文件名
    current_date = datetime.datetime.now().strftime('%Y%m%d')
    filename = f'{current_date}_选股结果Test.csv'

    all_results.to_csv(filename, index=False)  # 写入CSV文件

# 主函数
if __name__ == "__main__":
    global pbar  # 定义全局的进度条变量
    stock_codes = get_all_stock_codes()  # 获取所有的股票代码
    pbar = tqdm(total=len(stock_codes))  # 初始化进度条
    run_all_strategies(stock_codes)  # 运行所有的策略
