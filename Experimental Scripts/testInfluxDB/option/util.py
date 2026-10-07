# 此脚本是用python写的命令行工具

import argparse
import json
import os
import re
import matplotlib.pyplot as plt
import numpy as np
plt.rcParams["font.sans-serif"]=["SimHei"] #设置字体
plt.rcParams["axes.unicode_minus"]=False #正常显示负号

##########################################
# 本工具主要提供三方面的功能
# （1）分析一个文件的结果
# （2）分析old或者group的三个对比结果
# （3）分析对比结果
#########################################
# 比较案例
# （1）分析单个文件 python util.py -type group -case devops -index 1 -option analyse_cache
# （2）分析一个文件夹 python util.py -type group -case cpu-only -dict D:\MyWorkPlace\Python\influxdb\group\cpu-group -draw cache
# （3）对比 python util.py -case devops -cmp throughput -od ../group/devops-old -gd ../group/devops-group
########################################

use_case_type = ['cpu-only', 'devops', 'iot']  # 测例类型
data_file_names = {  # 对应用例的测试数据名称
    'cpu-only': ['influx-cpu-only-1000', 'influx-cpu-only-10000', 'influx-cpu-only-100000'],
    'devops': ['influx-devops-1000', 'influx-devops-10000', 'influx-devops-100000'],
    'iot': ['influx-iot-10000','influx-iot-100000']
}

dev_count = {  # 和数据集对应的设备数量
    'cpu-only': ['1000', '10000', '100000'],
    'devops': ['1000', '10000', '100000'],
    'iot': ['10000','100000']
}

cache_suffix = '-cache.log'
index_suffix = '-index.log'
time_suffix = '-time.log'
throughput_suffix = '-result.txt'

tsbs_res_path = "../group/devops-group/tsbs-res"  # tsbs测试结果的路径 主要关注吞吐量， 里面分为三个测例
result_path = "../group/devops-group/result"  # 测试结果 cache index time，分测例建文件夹

tsbs_res_dict = 'tsbs-res'
result_dict = 'result'


def main():
    parser = argparse.ArgumentParser(description='Your command line tool description')

    # 添加命令行参数

    # 公共的
    parser.add_argument('-type', type=str, help='组标签还是原来的方案，组标签（group），原来的方案（old）')
    parser.add_argument('-case', type=str, help='哪一个测例')

    # 分析单个文件
    parser.add_argument('-index', type=str, help='这个测例的第几个数据文件')
    parser.add_argument('-option', type=str, help='分析的操作')

    # 分析一组数据，输出加画图，就是指定一个文件夹，指定一个用例，画出这个测试的对比
    parser.add_argument('-draw', type=str, help='输出并画图，值有cache index time throughput')
    parser.add_argument('-dict', type=str, help='指定一个目录，该目录下必须有tsbs-res、result')

    # 分析对比数据，需要指定两个文件夹 然后选择一个用例，选择一个类型（draw）
    parser.add_argument('-cmp', type=str, help='输出并画图，值有cache index time throughput')
    parser.add_argument('-od', type=str, help='原来方案文件夹')
    parser.add_argument('-gd', type=str, help='优化后的文件夹')


    # 解析命令行参数
    args = parser.parse_args()

    # 执行相应的操作
    process_input(args.type, args.case, args.index, args.option, args.dict, args.draw, args.cmp, args.od, args.gd)  # 根据操作类型和用例类型做操作

# 根据case和index分析一个文件的index情况
def analyse_index_old(case, index):
    global result_path
    global data_file_names
    global index_suffix
    file_name = data_file_names[case][int(index) - 1] # 获取到第几个用例的第几个文件
    path = result_path + '/' + case + '/' + file_name +  index_suffix

    temp_res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符

    temp_res.reverse()

    res = []
    for line in temp_res:
        # 在这里执行你的操作，例如检查包含特定内容的行
        if "Database Name" in line:
            break
        res.append(line)
    # 处理结果
    for line in res:
        if "倒排列表总长度" in line:
            format_out_line(line)
        elif "倒排索引基数" in line:
            format_out_line(line)
        elif "倒排索引大小（lwq统计）" in line:
            format_out_size_line(line)
        elif "Index Series个数" in line:
            format_out_line(line)
        elif "Index Series内存占用" in line:
            format_out_size_line(line)

def format_out_kv(k, v):
    print(k + "=" + v)

# 原样输出
def format_out_line(line):
    k, v = get_key_value(line)
    print(k + "=" + v)

# 输出大小的
def format_out_size_line(line):
    k, v = get_key_value(line)
    mb_value = round(float(v) / 1024 / 1024, 2)  # 转换成MB
    format_out_kv(k, v + "B")
    format_out_kv(k, str(mb_value) + "MB")

# 获取冒号分割的两部分，方便转换成数字
def get_key_value(s):
    res = re.split(r'[:：]', s)
    return res[0], res[1]

# 根据case和index分析一个文件的cache情况
def analyse_cache_old(case, index):
    global result_path
    global data_file_names
    global cache_suffix
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = result_path + '/' + case + '/' + file_name + cache_suffix
    temp_res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符


    key_size = 0
    cache_size = 0
    res = [0,0,0,0,0,0] # 按字符串顺序
    cnt = 0
    # # 处理结果
    for i in range(len(temp_res)):
        if '+++++++++++++' in temp_res[i]:
            cnt += 1
        elif "Cache(自己统计)内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            cache_size = float(v)
            res[0] += int(v)
        elif "Cache Key内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            key_size = float(v)
            res[1] += int(v)
        elif "Cache Value内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[2] += int(v)
        elif "Cache Key数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[3] += int(v)
        elif "Cache Value数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[4] += int(v)
        elif "Cache 统计的count" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[5] += int(v)
    format_out_size_line('Cache(自己统计)内存总占用:' + str(int(int(res[0])/cnt)))
    format_out_size_line('Cache Key内存总占用:' + str(int(int(res[1])/cnt)))
    format_out_size_line('Cache Value内存总占用:' + str(int(int(res[2])/cnt)))
    format_out_line('Cache Key数量:' + str(int(int(res[3])/cnt)))
    format_out_line('Cache Value数量:' + str(int(int(res[4])/cnt)))
    format_out_line('Cache 统计的count:' + str(int(int(res[5])/cnt)))
    format_out_line("刷盘次数：" + str(int(len(temp_res)/7)))
    format_out_line("Key比例：" + str(round(key_size/cache_size, 4)))

def analyse_index_group(case, index):
    global result_path
    global data_file_names
    global index_suffix
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = result_path + '/' + case + '/' + file_name + index_suffix

    temp_res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符

    temp_res.reverse()

    res = []
    for line in temp_res:
        # 在这里执行你的操作，例如检查包含特定内容的行
        if "Database Name" in line:
            break
        res.append(line)

    all_series_size = 0
    all_index_size = 0
    # 处理结果
    for line in res:
        if "(组)个数" in line:
            format_out_line(line)
        elif "(组)倒排索引基数" in line:
            format_out_line(line)
        elif "(组)倒排索引大小（lwq统计）" in line:
            k, v = get_key_value(line)
            all_index_size += int(v)
            format_out_size_line(line)
        elif "(组)倒排列表总长度" in line:
            format_out_line(line)
        elif "(组)内存占用：78948" in line:
            k, v = get_key_value(line)
            all_series_size += int(v)
            format_out_size_line(line)
        elif "(序列)个数" in line:
            format_out_line(line)
        elif "(序列)倒排索引基数" in line:
            format_out_line(line)
        elif "(序列)倒排索引大小（lwq统计）" in line:
            k, v = get_key_value(line)
            all_index_size += int(v)
            format_out_size_line(line)
        elif "(序列)倒排列表总长度" in line:
            format_out_line(line)
        elif "(序列)内存占用：78948" in line:
            k, v = get_key_value(line)
            all_series_size += int(v)
            format_out_size_line(line)
    format_out_size_line('(总)序列数据结构内存占用：'+str(all_series_size))
    format_out_size_line('(总)倒排索引内存占用：'+str(all_index_size))

def analyse_cache_group(case, index):
    global result_path
    global data_file_names
    global cache_suffix
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = result_path + '/' + case + '/' + file_name + cache_suffix
    temp_res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符

    key_size = 0
    cache_size = 0
    res = [0, 0, 0, 0, 0, 0,0,0]  # 按字符串顺序
    cnt = 0
    # # 处理结果
    for i in range(len(temp_res)):
        if '+++++++++++++' in temp_res[i]:
            cnt += 1
        elif "Cache(Influxdb自己统计)内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            cache_size = float(v)
            res[0] += int(v)
        elif "Cache Key内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            key_size = float(v)
            res[1] += int(v)
        elif "Cache Value内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[2] += int(v)
        elif "Cache GroupKey内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[3] += int(v)
        elif "Cache SequenceKey内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[4] += int(v)
        elif "Cache GroupKey数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[5] += int(v)
        elif "Cache SequenceKey数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[6] += int(v)
        elif "Cache Value数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            res[7] += int(v)

    format_out_size_line('Cache(Influxdb自己统计)内存总占用:' + str(int(int(res[0]) / cnt)))
    format_out_size_line('Cache Key内存总占用:' + str(int(int(res[1]) / cnt)))
    format_out_size_line('Cache Value内存总占用:' + str(int(int(res[2]) / cnt)))
    format_out_size_line('Cache GroupKey内存总占用:' + str(int(int(res[3]) / cnt)))
    format_out_size_line('Cache SequenceKey内存总占用:' + str(int(int(res[4]) / cnt)))
    format_out_line('Cache GroupKey数量:' + str(int(int(res[5]) / cnt)))
    format_out_line('Cache SequenceKey数量:' + str(int(int(res[6]) / cnt)))
    format_out_line('Cache Value数量:' + str(int(int(res[7]) / cnt)))

    format_out_line("刷盘次数：" + str(int(len(temp_res) / 9)))
    format_out_line("Key比例：" + str(round(key_size / cache_size, 4)))

# # 根据case和index分析一个文件的i倒排索引维护时间情况
def analyse_index_time(case, index):
    global result_path
    global data_file_names
    global time_suffix
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = result_path + '/' + case + '/' + file_name + time_suffix
    res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符
    sum = 0
    for line in res:
        sum += get_time_long(line)
    print("倒排索引维护时间：" + str(int(sum / 1000000)) + " ms")
    print("倒排索引维护时间：" + str(int(sum / 1000000000)) + " s")

def get_time_long(data):
    start_index = data.find("start=") + len("start=")
    end_index = data.find(", end=")

    start_value = int(data[start_index:end_index])

    end_index += len(", end=")
    end_value = int(data[end_index:])

    # 计算 end - start
    result = end_value - start_value
    return result

# # 根据case和index分析一个文件的cache index time 情况
def analyse_all_old(case, index):
    print("######################  cache        ######################")
    analyse_cache_old(case, index)
    print("######################  index        ######################")
    analyse_index_old(case, index)
    print("######################  index time   ######################")
    analyse_index_time(case, index)

# 输出哪个case哪个index的全部数据
def analyse_all_group(case, index):
    print("######################  cache        ######################")
    analyse_cache_group(case, index)
    print("######################  index        ######################")
    analyse_index_group(case, index)
    print("######################  index time   ######################")
    analyse_index_time(case, index)

# 根据case和index分析一个文件的吞吐量
def analyse_throughput(case, index):
    global tsbs_res_path
    global data_file_names
    global throughput_suffix
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = tsbs_res_path + '/' + case + '/' + file_name + throughput_suffix

    try:
        with open(path, 'r') as file:
            data = json.load(file)

        duration_millis = data.get('DurationMillis', None)
        metric_rate = data['Totals'].get('metricRate', None)
        row_rate = data['Totals'].get('rowRate', None)

        print("DurationMillis: " + str(duration_millis))
        print("metricRate: " + str(metric_rate))
        print("rowRate: " + str(row_rate))

    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error extracting metrics: {e}")

####################################################################################################################################################################

# 柱状图函数
def my_single_draw(data, labels, title, xlabel):
    # x轴位置
    x = np.arange(len(labels))
    bar_width = 0.2
    # 创建A和B的柱状图
    plt.bar(x, data, bar_width, label=title)
    # 添加标签
    plt.xlabel(xlabel)
    plt.ylabel('')
    plt.title(title)
    plt.xticks(x, labels)
    plt.legend()
    # 显示图形
    plt.show()

# 返回单个文件的[刷盘次数，Key比例，value个数]
def get_cache_data_old(case, index, dict):
    global data_file_names
    global cache_suffix
    global result_dict
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = dict + '/' + result_dict + '/' + case + '/' + file_name + cache_suffix
    temp_res = []
    if not os.path.exists(path):
        return 0, 0, 0
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符

    max_value = 0  # 最大总内存
    max_index = 1  # 记录最大的cache内存索引

    key_size = 0
    cache_size = 0
    value_cnt = 0
    cnt = 0
    # # 处理结果
    for i in range(len(temp_res)):
        if '+++++++++++++' in temp_res[i]:
            cnt += 1
        elif "Cache(自己统计)内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            cache_size += int(v)
        elif "Cache Key内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            key_size += int(v)
        elif "Cache Value数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            value_cnt = int(v)

    return cnt, str(round(key_size/cache_size, 4)), int(value_cnt/cnt)

# cache主要包括[刷盘次数，Key比例，value个数]
def draw_cache_old(case, dict):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case]) # 数据个数

    flush_cnt = [] #刷盘次数
    key_ratio = [] # key比例
    value_cnt = [] # value个数

    # 从dict目录读取数据
    for i in range(length):
        a ,b, c = get_cache_data_old(case, i + 1, dict)
        flush_cnt.append(int(a))
        key_ratio.append(round(float(b),2))
        value_cnt.append(int(c))

    print('刷盘次数：', flush_cnt)
    my_single_draw(flush_cnt, dev_count[case], 'Cache刷盘次数', '设备量')
    print('key比例：', key_ratio)
    my_single_draw(key_ratio, dev_count[case], 'key比例', '设备量')
    print('value个数：', value_cnt)
    my_single_draw(value_cnt, dev_count[case], 'value个数', '设备量')

# [刷盘次数、key比例、groupkey比例、sequencekey比例、groupkey数量、sequencekey数量、value个数]
def get_cache_data_group(case, index, dict):
    global data_file_names
    global cache_suffix
    global result_dict
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = dict + '/' + result_dict + '/' + case + '/' + file_name + cache_suffix
    print(path)
    temp_res = []
    if not os.path.exists(path):
        return 0,0,0,0,0,0,0
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符


    data = [0,0,0,0,0,0,0,0]
    cnt = 0

    for i in range(len(temp_res)):
        if '+++++++++++++' in temp_res[i]:
            cnt += 1
        elif "Cache(Influxdb自己统计)内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[0] += int(v)
        elif "Cache Key内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[1] += int(v)
        elif "Cache Value内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[2] += int(v)
        elif "Cache GroupKey内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[3] += int(v)
        elif "Cache SequenceKey内存总占用" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[4] += int(v)
        elif "Cache GroupKey数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[5] += int(v)
        elif "Cache SequenceKey数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[6] += int(v)
        elif "Cache Value数量" in temp_res[i]:
            k, v = get_key_value(temp_res[i])
            data[7] += int(v)

    return cnt, \
           str(round(data[1]/data[0], 4)), \
            str(round(data[3]/data[0], 4)), \
            str(round(data[4]/data[0], 4)), \
           int(data[5] / cnt), int(data[6]/cnt),int(data[7]/cnt)

# [刷盘次数、key比例、groupkey比例、sequencekey比例、groupkey数量、sequencekey数量、value个数]
def draw_cache_group(case, dict):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    data = [[],[],[],[],[],[],[]] # 7个指标

    # 从dict目录读取数据
    for i in range(length):
        a1,a2,a3,a4,a5,a6,a7 = get_cache_data_group(case, i + 1, dict)
        data[0].append(int(a1))
        data[1].append(float(a2))
        data[2].append(float(a3))
        data[3].append(float(a4))
        data[4].append(int(a5))
        data[5].append(int(a6))
        data[6].append(int(a7))

    print('刷盘次数：', data[0])
    my_single_draw(data[0], dev_count[case], '刷盘次数', '设备量')
    print('key比例：', data[1])
    my_single_draw(data[1], dev_count[case], 'key比例', '设备量')
    print('groupkey比例：', data[2])
    my_single_draw(data[2], dev_count[case], 'groupkey比例', '设备量')
    print('sequencekey比例：', data[3])
    my_single_draw(data[3], dev_count[case], 'sequencekey比例', '设备量')
    print('groupkey数量：', data[4])
    my_single_draw(data[4], dev_count[case], 'groupkey数量', '设备量')
    print('sequencekey数量：', data[5])
    my_single_draw(data[5], dev_count[case], 'sequencekey数量', '设备量')
    print('value个数：', data[6])
    my_single_draw(data[6], dev_count[case], 'value个数', '设备量')

# index主要包括[倒排列表总长度，倒排索引基数，倒排索引大小，Index Series个数，Index Series内存占用]
def get_index_data_old(case, index, dict):
    global data_file_names
    global index_suffix
    global result_dict
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = dict + '/' + result_dict + '/' + case + '/' + file_name + index_suffix

    temp_res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符

    temp_res.reverse()

    temp = []
    for line in temp_res:
        # 在这里执行你的操作，例如检查包含特定内容的行
        if "Database Name" in line:
            break
        temp.append(line)
    # 处理结果
    res = [0,0,0,0,0]
    for line in temp:
        if "倒排列表总长度" in line:
            k, v = get_key_value(line)
            res[0] = int(v)
        elif "倒排索引基数" in line:
            k, v = get_key_value(line)
            res[1] = int(v)
        elif "倒排索引大小（lwq统计）" in line:
            k, v = get_key_value(line)
            v = round(float(v) / 1024 , 2)  # 转换成KB
            res[2] = int(v)
        elif "Index Series个数" in line:
            k, v = get_key_value(line)
            res[3] = int(v)
        elif "Index Series内存占用" in line:
            k, v = get_key_value(line)
            v = round(float(v) / 1024 , 2)  # 转换成KB
            res[4] = int(v)
    return res[0], res[1],res[2],res[3],res[4]

# index主要包括[组倒排列表总长度，组倒排索引基数，组倒排索引大小，组Series内存占用，
# 序列倒排列表总长度，序列排索引基数，序列排索引大小，序列Series内存占用
# 总排列表总长度，总倒排索引基数，总倒排索引大小，总Series内存占用]
def get_index_data_group(case, index, dict):
    global data_file_names
    global index_suffix
    global result_dict
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = dict + '/' + result_dict + '/' + case + '/' + file_name + index_suffix

    temp_res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            temp_res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符

    temp_res.reverse()

    temp = []
    for line in temp_res:
        # 在这里执行你的操作，例如检查包含特定内容的行
        if "Database Name" in line:
            break
        temp.append(line)
    # 处理结果

    res = [0,0,0,0,0,0,0,0,0,0]
    for line in temp:
        if "(组)个数" in line:
            k, v = get_key_value(line)
            res[0] = int(v)
        elif "(组)内存占用" in line:
            k, v = get_key_value(line)
            v = round(float(v) / 1024, 2)  # 转换成KB
            res[1] = int(v)
        elif "(组)倒排索引大小" in line:
            k, v = get_key_value(line)
            v = round(float(v) / 1024 , 2)  # 转换成KB
            res[2] = int(v)
        elif "(组)倒排索引基数" in line:
            k, v = get_key_value(line)
            res[3] = int(v)
        elif "(组)倒排列表总长度" in line:
            k, v = get_key_value(line)
            res[4] = int(v)
        elif "(序列)个数" in line:
            k, v = get_key_value(line)
            res[5] = int(v)
        elif "(序列)内存占用" in line:
            k, v = get_key_value(line)
            v = round(float(v) / 1024, 2)  # 转换成KB
            res[6] = int(v)
        elif "(序列)倒排索引大小" in line:
            k, v = get_key_value(line)
            v = round(float(v) / 1024 , 2)  # 转换成KB
            res[7] = int(v)
        elif "(序列)倒排索引基数" in line:
            k, v = get_key_value(line)
            res[8] = int(v)
        elif "(序列)倒排列表总长度" in line:
            k, v = get_key_value(line)
            res[9] = int(v)

    # index主要包括[组倒排列表总长度，组倒排索引基数，组倒排索引大小，组Series内存占用，
    # 序列倒排列表总长度，序列排索引基数，序列排索引大小，序列Series内存占用
    # 总排列表总长度，总倒排索引基数，总倒排索引大小，总Series内存占用]
    result = []
    result.append(res[4])
    result.append(res[3])
    result.append(res[2])
    result.append(res[1])
    result.append(res[9])
    result.append(res[8])
    result.append(res[7])
    result.append(res[5])
    result.append(res[4] + res[9])
    result.append(res[3] + res[8])
    result.append(res[2] + res[7])
    result.append(res[1] + res[6])
    return result

# index主要包括[倒排列表总长度，倒排索引基数，倒排索引大小，Index Series个数，Index Series内存占用]
def draw_index_old(case, dict):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    data = [[],[],[],[],[]] # 按顺序对应

    # 从dict目录读取数据
    for i in range(length):
        a, b, c, d, e = get_index_data_old(case, i + 1, dict)
        data[0].append(a)
        data[1].append(b)
        data[2].append(c)
        data[3].append(d)
        data[4].append(e)

    print('倒排列表总长度：', data[0])
    my_single_draw(data[0], dev_count[case], '倒排列表总长度', '设备量')
    print('倒排索引基数：', data[1])
    my_single_draw(data[1], dev_count[case], '倒排索引基数', '设备量')
    print('倒排索引大小：', data[2])
    my_single_draw(data[2], dev_count[case], '倒排索引大小', '设备量')
    print('Index Series个数：', data[3])
    my_single_draw(data[3], dev_count[case], 'Index Series个数', '设备量')
    print('Index Series内存占用：', data[4])
    my_single_draw(data[4], dev_count[case], 'Index Series内存占用', '设备量')

# 返回毫秒与秒
def get_index_time(case, index, dict):
    global result_path
    global data_file_names
    global time_suffix
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = dict + '/' + result_dict + '/' + case + '/' + file_name + time_suffix
    res = []
    if not os.path.exists(path):
        return
    with open(path, 'r', encoding='utf-8') as file:
        lines = file.readlines()
        for line in lines:
            res.append(line.strip())  # 使用 append 方法添加元素，并去除行末尾的换行符
    sum = 0
    for line in res:
        sum += get_time_long(line)
    return str(int(sum / 1000000)), str(int(sum / 1000000000))

# 索引构建时间：返回值[毫秒，秒]
def draw_index_time(case, dict):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    data = [[], []]  # 按顺序对应

    # 从dict目录读取数据
    for i in range(length):
        a, b= get_index_time(case, i + 1, dict)
        data[0].append(int(a))
        data[1].append(int(b))


    print('索引构建时间(ms)：', data[0])
    my_single_draw(data[0], dev_count[case], '索引构建时间(ms)', '设备量')
    print('索引构建时间(s)：', data[1])
    my_single_draw(data[1], dev_count[case], '索引构建时间(s)', '设备量')

# 吞吐率：[DurationMillis, metricRate, rowRate]
def get_throughput(case, index, dict):
    global tsbs_res_path
    global data_file_names
    global throughput_suffix
    global tsbs_res_dict
    file_name = data_file_names[case][int(index) - 1]  # 获取到第几个用例的第几个文件
    path = dict + '/' + tsbs_res_dict + '/' + case + '/' + file_name + throughput_suffix

    if not os.path.exists(path):
        return 0,0,0
    try:
        with open(path, 'r') as file:
            data = json.load(file)
        duration_millis = data.get('DurationMillis', None)
        metric_rate = data['Totals'].get('metricRate', None)
        row_rate = data['Totals'].get('rowRate', None)
        return str(duration_millis), str(metric_rate),str(row_rate)
    except (json.JSONDecodeError, KeyError) as e:
        print(f"Error extracting metrics: {e}")
        return 0,0,0

# 吞吐率：[DurationMillis, metricRate, rowRate]
def draw_throughput(case, dict):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    data = [[], [], []]  # 按顺序对应

    # 从dict目录读取数据
    for i in range(length):
        a, b,c = get_throughput(case, i + 1, dict)
        data[0].append(float(a))
        data[1].append(float(b))
        data[2].append(float(c))

    print('DurationMillis：', data[0])
    my_single_draw(data[0], dev_count[case], 'DurationMillis', '设备量')
    print('metricRate：', data[1])
    my_single_draw(data[1], dev_count[case], 'metricRate', '设备量')
    print('rowRate：', data[2])
    my_single_draw(data[2], dev_count[case], 'rowRate', '设备量')

# index主要包括[组倒排列表总长度，组倒排索引基数，组倒排索引大小，组Series内存占用，
# 序列倒排列表总长度，序列排索引基数，序列排索引大小，序列Series内存占用
# 总排列表总长度，总倒排索引基数，总倒排索引大小，总Series内存占用]
def draw_index_group(case, dict):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    data = [[],[],[],[],[],[],[],[],[],[],[],[]]  # 按顺序对应

    # 从dict目录读取数据
    for i in range(length):
        res_list = get_index_data_group(case, i + 1, dict)
        data[0].append(res_list[0])
        data[1].append(res_list[1])
        data[2].append(res_list[2])
        data[3].append(res_list[3])
        data[4].append(res_list[4])
        data[5].append(res_list[5])
        data[6].append(res_list[6])
        data[7].append(res_list[7])
        data[8].append(res_list[8])
        data[9].append(res_list[9])
        data[10].append(res_list[10])
        data[11].append(res_list[11])

    # index主要包括[，，，，
    # ，，，
    # ，，，]
    print('组倒排列表总长度：', data[0])
    my_single_draw(data[0], dev_count[case], '组倒排列表总长度', '设备量')

    print('组倒排索引基数：', data[1])
    my_single_draw(data[1], dev_count[case], '组倒排索引基数', '设备量')

    print('组倒排索引大小：', data[2])
    my_single_draw(data[2], dev_count[case], '组倒排索引大小', '设备量')

    print('组Series内存占用：', data[3])
    my_single_draw(data[3], dev_count[case], '组Series内存占用', '设备量')

    print('序列倒排列表总长度：', data[4])
    my_single_draw(data[4], dev_count[case], '序列倒排列表总长度', '设备量')

    print('序列排索引基数：', data[5])
    my_single_draw(data[5], dev_count[case], '序列排索引基数', '设备量')

    print('序列排索引大小：', data[6])
    my_single_draw(data[6], dev_count[case], '序列排索引大小', '设备量')

    print('序列Series内存占用：', data[7])
    my_single_draw(data[7], dev_count[case], '序列Series内存占用', '设备量')

    print('总排列表总长度：', data[8])
    my_single_draw(data[8], dev_count[case], '总排列表总长度', '设备量')

    print('总倒排索引基数：', data[9])
    my_single_draw(data[9], dev_count[case], '总倒排索引基数', '设备量')

    print('总倒排索引大小：', data[10])
    my_single_draw(data[10], dev_count[case], '总倒排索引大小', '设备量')

    print('总Series内存占用：', data[11])
    my_single_draw(data[11], dev_count[case], '总Series内存占用', '设备量')


####################################################################################################################################################################
# 比较,画出双列柱状图
def draw_double(data1, data2, labels, title, xlabel):
    # x轴位置
    x = np.arange(len(labels))
    bar_width = 0.2
    # 创建A和B的柱状图
    plt.bar(x - bar_width/2, data1, bar_width, label='原先方案')
    plt.bar(x + bar_width/2, data2, bar_width, label='优化方案')
    # 添加标签
    plt.xlabel(xlabel)
    plt.ylabel('')
    plt.title(title)
    plt.xticks(x, labels)
    plt.legend()
    # 显示图形
    plt.show()

# 画出每个case下的cache结果对比，主要有如下数据 [刷盘次数、key比例、value个数]
def draw_cache_cmp(case,cmp, od, gd):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    flush_data = [[], []]  # 刷盘次数 0为old 1 为group
    key_ratio_data = [[], []]  # key比例
    value_cnt_data = [[], []]  # value个数

    # 从dict目录读取数据
    for i in range(length):
        o0, o1, o2 = get_cache_data_old(case, i + 1, od) # old对应 0 1 2
        g0, g1, _, _, _, _, g6 = get_cache_data_group(case, i + 1, gd) # group对应 0 1 6
        flush_data[0].append(int(o0))
        flush_data[1].append(int(g0))
        key_ratio_data[0].append(round(float(o1) ,4))
        key_ratio_data[1].append(round(float(g1) ,4))
        value_cnt_data[0].append(int(o2))
        value_cnt_data[1].append(int(g6))

    print("对比：Cache刷盘次数")
    print("old:", flush_data[0])
    print("group:", flush_data[1])
    draw_double(flush_data[0], flush_data[1], dev_count[case], 'Cache刷盘次数', '设备量')

    print("对比：Cache Key比例")
    print("old:", key_ratio_data[0])
    print("group:", key_ratio_data[1])
    draw_double(key_ratio_data[0], key_ratio_data[1], dev_count[case], 'Cache Key比例', '设备量')

    print("对比：Cache Value个数")
    print("old:", value_cnt_data[0])
    print("group:", value_cnt_data[1])
    draw_double(value_cnt_data[0], value_cnt_data[1], dev_count[case], 'Cache Value个数', '设备量')

#index主要比较[倒排列表总长度，倒排索引大小，Series内存占用，总索引占用，维护时间]
def draw_index_cmp(case, cmp, od, gd):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    invert_len = [[], []]  # 倒排列表总长度 0为old 1 为group
    invert_size = [[], []] # 倒排索引大小
    series_size = [[], []] # Series内存占用
    sum_size = [[], []] # 总索引占用
    maintain_time_milli = [[], []] # 维护时间 ms
    maintain_time_second = [[], []] # 维护时间 s

    # 从dict目录读取数据
    for i in range(length):
        # old index主要包括[倒排列表总长度，倒排索引基数，倒排索引大小，Index Series个数，Index Series内存占用]
        # group # index主要包括[组倒排列表总长度，组倒排索引基数，组倒排索引大小，组Series内存占用，
        # # 序列倒排列表总长度，序列排索引基数，序列排索引大小，序列Series内存占用
        # # 总排列表总长度，总倒排索引基数，总倒排索引大小，总Series内存占用]

        # 获得倒排列表总长度，倒排索引大小，Series内存占用  总的就是（倒排索引大小 + Series内存占用，自己计算） 大小单位都是kb
        o0, _, o2, _, o4 = get_index_data_old(case, i + 1, od)  # old对应 0 2 4
        g_res = get_index_data_group(case, i + 1, gd)  # group对应 8 10 11
        invert_len[0].append(int(o0))
        invert_len[1].append(int(g_res[8]))
        invert_size[0].append(int(o2))
        invert_size[1].append(int(g_res[10]))
        series_size[0].append(int(o4))
        series_size[1].append(int(g_res[11]))
        sum_size[0].append(int(o2) + int(o4))
        sum_size[1].append(int(g_res[10]) + int(g_res[11]))

        ot0, ot1 = get_index_time(case, i + 1, od) # 返回毫秒 秒
        gt0, gt1 = get_index_time(case, i + 1, gd) # 返回毫秒 秒
        maintain_time_milli[0].append(int(ot0))
        maintain_time_milli[1].append(int(gt0))
        maintain_time_second[0].append(int(ot1))
        maintain_time_second[1].append(int(gt1))

    # 索引的指标
    print("对比：倒排列表总长度")
    print("old:", invert_len[0])
    print("group:", invert_len[1])
    draw_double(invert_len[0], invert_len[1], dev_count[case], '倒排列表总长度', '设备量')

    print("对比：倒排索引大小")
    print("old:", invert_size[0])
    print("group:", invert_size[1])
    draw_double(invert_size[0], invert_size[1], dev_count[case], '倒排索引大小', '设备量')

    print("对比：Series内存占用")
    print("old:", series_size[0])
    print("group:", series_size[1])
    draw_double(series_size[0], series_size[1], dev_count[case], 'Series内存占用', '设备量')

    print("对比：总索引占用")
    print("old:", sum_size[0])
    print("group:", sum_size[1])
    draw_double(sum_size[0], sum_size[1], dev_count[case], '总索引占用', '设备量')

    # 索引维护时间
    print("对比：索引维护时间（单位ms）")
    print("old:", maintain_time_milli[0])
    print("group:", maintain_time_milli[1])
    draw_double(maintain_time_milli[0], maintain_time_milli[1], dev_count[case], '索引维护时间（单位ms）', '设备量')

    print("对比：索引维护时间（单位s）")
    print("old:", maintain_time_second[0])
    print("group:", maintain_time_second[1])
    draw_double(maintain_time_second[0], maintain_time_second[1], dev_count[case], '索引维护时间（单位s）', '设备量')

# 画出吞吐量比较，也是三个指标[DurationMillis, metricRate, rowRate]
def draw_cmp_throughput(case, cmp, od, gd):
    global result_path
    global data_file_names
    global cache_suffix
    global dev_count
    global tsbs_res_dict
    global result_dict
    length = len(data_file_names[case])  # 数据个数

    duration_millis = [[], []]  # 0为old 1 为group
    metric_rate = [[], []]
    row_rate = [[], []]

    # 从dict目录读取数据
    for i in range(length):
        o0, o1, o2 = get_throughput(case, i + 1, od)  # old对应 0 1 2
        g0, g1, g2 = get_throughput(case, i + 1, gd)  # group对应 0 1 2
        duration_millis[0].append(round(float(o0), 4))
        duration_millis[1].append(round(float(g0), 4))
        metric_rate[0].append(round(float(o1), 4))
        metric_rate[1].append(round(float(g1), 4))
        row_rate[0].append(round(float(o2), 4))
        row_rate[1].append(round(float(g2), 4))

    print("对比：DurationMillis")
    print("old:", duration_millis[0])
    print("group:", duration_millis[1])
    draw_double(duration_millis[0], duration_millis[1], dev_count[case], 'DurationMillis', '设备量')

    print("对比：metricRate")
    print("old:", metric_rate[0])
    print("group:", metric_rate[1])
    draw_double(metric_rate[0], metric_rate[1], dev_count[case], 'metricRate', '设备量')

    print("对比：rowRate")
    print("old:", row_rate[0])
    print("group:", row_rate[1])
    draw_double(row_rate[0], row_rate[1], dev_count[case], 'rowRate', '设备量')


def process_input(type, case, index, option, dict, draw, cmp, od, gd):

    # 单个文件操作
    if type == "old":
        if option == "analyse_index":
            analyse_index_old(case, index)
        elif option == "analyse_cache":
            analyse_cache_old(case, index)
        elif option == "analyse_index_time":
            analyse_index_time(case, index)
        elif option == "analyse_throughput": # 统计吞吐量
            analyse_throughput(case, index)
        elif option == "analyse_all":
            analyse_all_old(case, index)
    elif type == "group":
        if option == "analyse_index":
            analyse_index_group(case, index)
        elif option == "analyse_cache":
            analyse_cache_group(case, index)
        elif option == "analyse_index_time":
            analyse_index_time(case, index)
        elif option == "analyse_throughput": # 统计吞吐量
            analyse_throughput(case, index)
        elif option == "analyse_all":
            analyse_all_group(case, index)

    # 单个文件夹操作，输出数据并画图
    if type == "old":
        if draw == "cache":
            draw_cache_old(case, dict)
        elif draw == "index":
            draw_index_old(case, dict)
        elif draw == 'time':
            draw_index_time(case, dict)
        elif draw == 'throughput':
            draw_throughput(case, dict)
    elif type == "group":
        if draw == "cache":
            draw_cache_group(case, dict)
        elif draw == "index":
            draw_index_group(case, dict)
        elif draw == 'time':
            draw_index_time(case, dict)
        elif draw == 'throughput':
            draw_throughput(case, dict)

    # 结果比较
    if cmp == 'cache':
        draw_cache_cmp(case, cmp, od, gd)
    elif cmp == 'index':
        draw_index_cmp(case, cmp, od, gd) # 包含索引维护时间
    elif cmp == 'throughput':
        draw_cmp_throughput(case, cmp, od, gd)



def parseValue(option):
    path_option = option.split(" ")[0].replace("\\\\", "\\")
    return path_option.split("=")[1]


if __name__ == '__main__':
    # 运行
    main()
    # draw_cache_cmp('devops','cache', '../group/devops-old', '../group/devops-group')

