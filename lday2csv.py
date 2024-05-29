import os
import struct
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed  # 直接从futures导入as_completed

def process_file(src_path, dst_path, market):
    """单个文件处理函数，用于多线程"""
    try:
        stock_csv(src_path, os.path.basename(src_path)[:-4], dst_path)
        print(f"完成处理: {src_path} -> {dst_path}")
    except Exception as e:
        print(f"处理失败: {src_path} -> {dst_path}, 错误: {e}")


def process_directory(src_dir, dst_base_dir, market, max_workers=5):
    """处理指定目录下的所有.day文件，并将CSV输出到指定的基础目录下对应market的子目录中，使用多线程加速"""
    dst_dir = os.path.join(dst_base_dir, market)
    if not os.path.exists(dst_dir):
        os.makedirs(dst_dir)
        
    tasks = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        for filename in os.listdir(src_dir):
            src_path = os.path.join(src_dir, filename)
            dst_file_name = filename[:-4] + '.csv'
            dst_path = os.path.join(dst_dir, dst_file_name)
            tasks.append(executor.submit(process_file, src_path, dst_path, market))
        
        # 等待所有任务完成
        for future in as_completed(tasks): 
            try:
                future.result()
            except Exception as exc:
                print(f"生成文件时发生了错误: {exc}")


def stock_csv(filepath, name, output_path):
    """解析单个.day文件并输出为CSV，包含列标题，数值保留两位小数"""
    with open(filepath, 'rb') as f:
        with open(output_path, 'w+', encoding='utf-8') as file_object:
            # 写入列标题
            file_object.write("Date,Open,High,Low,Close,Vol\n")
            
            while True:
                stock_date = f.read(4)
                if not stock_date:
                    break
                stock_open = f.read(4)
                stock_high = f.read(4)
                stock_low = f.read(4)
                stock_close = f.read(4)
                stock_amount = f.read(4)
                stock_vol = f.read(4)
                stock_reservation = f.read(4)
                
                stock_date = struct.unpack("l", stock_date)[0]
                stock_open = round(struct.unpack("l", stock_open)[0] / 100, 2)  # 保留两位小数
                stock_high = round(struct.unpack("l", stock_high)[0] / 100, 2)
                stock_low = round(struct.unpack("l", stock_low)[0] / 100, 2)
                stock_close = round(struct.unpack("l", stock_close)[0] / 100, 2)
                stock_amount = struct.unpack("f", stock_amount)[0]  # 成交额，保持浮点数
                stock_vol = struct.unpack("l", stock_vol)[0]  # 成交量，通常为整数，不需要格式化为两位小数
                
                date_format = datetime.datetime.strptime(str(stock_date), '%Y%m%d')
                line = f"{date_format.strftime('%Y-%m-%d')},{stock_open:.2f},{stock_high:.2f},{stock_low:.2f},{stock_close:.2f},{stock_vol}\n"
                
                file_object.write(line)


# 主处理流程
src_dirs = {
    'sh': 'D:/通达信/vipdoc/sh/lday',# 此处需要更改为通达信上海日线数据存储的实际目录
    'sz': 'D:/通达信/vipdoc/sz/lday'# 此处需要更改为通达信深圳日线数据存储的实际目录
}
dst_base_dir = 'D:/PyTdx/csv'# 此处需要更改为解析后的CSV文件的存储目录

for market, src_dir in src_dirs.items():
    if os.path.exists(src_dir):
        print(f"开始处理 {market.upper()} 市场数据...")
        process_directory(src_dir, dst_base_dir, market)
    else:
        print(f"警告：{src_dir} 路径不存在！")

print("所有处理完成。")
