import time
import requests


# InfluxDB 的配置
url = "http://localhost:8086/query"
db_name = "lwq"


queries5 = [
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T16:59:56.804110Z' ",
]
total_time5 = 0
count5 = 0

for query in queries5:
    # 构建查询参数
    params = {
        'db': db_name,
        'q': query
    }

    start_time = time.time()

    response = requests.post(url, data=params)

    end_time = time.time()

    # 计算执行时间
    if response.status_code == 200:
        total_time5 += end_time - start_time
        count5 += 1
        print(response.json())


# 输出结果
if total_time5 != 0:
    result5 = count5 / total_time5

print(f"result5: {result5:.6f} count5: {count5}")
print("Execution time 5: {:.6f} seconds".format(total_time5))


queries4 = [
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T08:59:56.804110Z' and time < '2024-07-17T12:59:56.804110Z' ",
]
total_time4 = 0
count4 = 0

for query in queries4:
    # 构建查询参数
    params = {
        'db': db_name,
        'q': query
    }

    start_time = time.time()

    response = requests.post(url, data=params)

    end_time = time.time()

    # 计算执行时间
    if response.status_code == 200:
        total_time4 += end_time - start_time
        count4 += 1
        print(response.json())


# 输出结果
if total_time4 != 0:
    result4 = count4 / total_time4

print(f"result4: {result4:.6f} count4: {count4}")
print("Execution time 4: {:.6f} seconds".format(total_time4))

queries3 = [
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
    # "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T09:59:56.804110Z' and time < '2024-07-17T10:59:56.804110Z' ",
]
total_time3 = 0
count3 = 0

for query in queries3:
    # 构建查询参数
    params = {
        'db': db_name,
        'q': query
    }

    start_time = time.time()

    response = requests.post(url, data=params)

    end_time = time.time()

    # 计算执行时间
    if response.status_code == 200:
        total_time3 += end_time - start_time
        count3 += 1
        print(response.json())


# 输出结果
if total_time3 != 0:
    result3 = count3 / total_time3

print(f"result3: {result3:.6f} count3: {count3}")
print("Execution time 3: {:.6f} seconds".format(total_time3))




queries1 = [
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:49:58.480433Z' and time < '2024-07-17T10:54:58.480433Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:17:51.633952Z' and time < '2024-07-17T10:22:51.633952Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:04:32.581179Z' and time < '2024-07-17T10:09:32.581179Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:03:02.462572Z' and time < '2024-07-17T10:08:02.462572Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:25:08.293026Z' and time < '2024-07-17T10:30:08.293026Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:19:31.494680Z' and time < '2024-07-17T10:24:31.494680Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:42:47.183066Z' and time < '2024-07-17T10:47:47.183066Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:20:32.362444Z' and time < '2024-07-17T10:25:32.362444Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:19:27.273059Z' and time < '2024-07-17T10:24:27.273059Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:49:02.120116Z' and time < '2024-07-17T10:54:02.120116Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:42:52.230272Z' and time < '2024-07-17T10:47:52.230272Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:02:08.529997Z' and time < '2024-07-17T10:07:08.529997Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:05:16.554338Z' and time < '2024-07-17T10:10:16.554338Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:24:49.298686Z' and time < '2024-07-17T10:29:49.298686Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:33:17.882063Z' and time < '2024-07-17T10:38:17.882063Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:22:17.912231Z' and time < '2024-07-17T10:27:17.912231Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:04:56.462756Z' and time < '2024-07-17T10:09:56.462756Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:41:17.092278Z' and time < '2024-07-17T10:46:17.092278Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:14:33.449169Z' and time < '2024-07-17T10:19:33.449169Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:00:22.115466Z' and time < '2024-07-17T10:05:22.115466Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:46:00.310564Z' and time < '2024-07-17T10:51:00.310564Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:43:11.841397Z' and time < '2024-07-17T10:48:11.841397Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:21:17.725110Z' and time < '2024-07-17T10:26:17.725110Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:31:06.442631Z' and time < '2024-07-17T10:36:06.442631Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:44:37.113255Z' and time < '2024-07-17T10:49:37.113255Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:24:09.479401Z' and time < '2024-07-17T10:29:09.479401Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:40:45.046777Z' and time < '2024-07-17T10:45:45.046777Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:38:05.559520Z' and time < '2024-07-17T10:43:05.559520Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:29:45.182308Z' and time < '2024-07-17T10:34:45.182308Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:48:31.702713Z' and time < '2024-07-17T10:53:31.702713Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:38:54.283388Z' and time < '2024-07-17T10:43:54.283388Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:23:51.208456Z' and time < '2024-07-17T10:28:51.208456Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:31:17.568362Z' and time < '2024-07-17T10:36:17.568362Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:15:48.012916Z' and time < '2024-07-17T10:20:48.012916Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:08:52.361194Z' and time < '2024-07-17T10:13:52.361194Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:10:59.378164Z' and time < '2024-07-17T10:15:59.378164Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:06:06.346873Z' and time < '2024-07-17T10:11:06.346873Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:11:02.098978Z' and time < '2024-07-17T10:16:02.098978Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:06:11.789049Z' and time < '2024-07-17T10:11:11.789049Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:22:46.638070Z' and time < '2024-07-17T10:27:46.638070Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:39:04.290697Z' and time < '2024-07-17T10:44:04.290697Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:13:51.927024Z' and time < '2024-07-17T10:18:51.927024Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:08:28.306321Z' and time < '2024-07-17T10:13:28.306321Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:23:34.844893Z' and time < '2024-07-17T10:28:34.844893Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:14:16.284707Z' and time < '2024-07-17T10:19:16.284707Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:04:12.996648Z' and time < '2024-07-17T10:09:12.996648Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:09:20.071574Z' and time < '2024-07-17T10:14:20.071574Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:26:03.879862Z' and time < '2024-07-17T10:31:03.879862Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:27:11.666891Z' and time < '2024-07-17T10:32:11.666891Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:49:38.965340Z' and time < '2024-07-17T10:54:38.965340Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:17:05.711384Z' and time < '2024-07-17T10:22:05.711384Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:15:44.144452Z' and time < '2024-07-17T10:20:44.144452Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:37:26.027596Z' and time < '2024-07-17T10:42:26.027596Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:21:44.409304Z' and time < '2024-07-17T10:26:44.409304Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:06:35.425336Z' and time < '2024-07-17T10:11:35.425336Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:39:33.609150Z' and time < '2024-07-17T10:44:33.609150Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:13:42.679090Z' and time < '2024-07-17T10:18:42.679090Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:20:58.901065Z' and time < '2024-07-17T10:25:58.901065Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:24:46.210905Z' and time < '2024-07-17T10:29:46.210905Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:50:27.561337Z' and time < '2024-07-17T10:55:27.561337Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:39:57.042611Z' and time < '2024-07-17T10:44:57.042611Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:44:46.485018Z' and time < '2024-07-17T10:49:46.485018Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:28:03.453680Z' and time < '2024-07-17T10:33:03.453680Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:45:10.554752Z' and time < '2024-07-17T10:50:10.554752Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:04:55.749599Z' and time < '2024-07-17T10:09:55.749599Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:30:48.771711Z' and time < '2024-07-17T10:35:48.771711Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:19:41.488229Z' and time < '2024-07-17T10:24:41.488229Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:34:19.418044Z' and time < '2024-07-17T10:39:19.418044Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:19:15.177637Z' and time < '2024-07-17T10:24:15.177637Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:09:11.575620Z' and time < '2024-07-17T10:14:11.575620Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:32:10.946424Z' and time < '2024-07-17T10:37:10.946424Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:03:49.078144Z' and time < '2024-07-17T10:08:49.078144Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:43:21.195779Z' and time < '2024-07-17T10:48:21.195779Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:45:44.360865Z' and time < '2024-07-17T10:50:44.360865Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:49:01.087558Z' and time < '2024-07-17T10:54:01.087558Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:07:51.624945Z' and time < '2024-07-17T10:12:51.624945Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:49:23.278248Z' and time < '2024-07-17T10:54:23.278248Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:37:23.086522Z' and time < '2024-07-17T10:42:23.086522Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:31:07.517420Z' and time < '2024-07-17T10:36:07.517420Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:30:07.948853Z' and time < '2024-07-17T10:35:07.948853Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:16:19.708622Z' and time < '2024-07-17T10:21:19.708622Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:26:18.284470Z' and time < '2024-07-17T10:31:18.284470Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:24:14.810659Z' and time < '2024-07-17T10:29:14.810659Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:29:32.056743Z' and time < '2024-07-17T10:34:32.056743Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:34:11.096972Z' and time < '2024-07-17T10:39:11.096972Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:47:17.270445Z' and time < '2024-07-17T10:52:17.270445Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:11:22.762987Z' and time < '2024-07-17T10:16:22.762987Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:35:21.362575Z' and time < '2024-07-17T10:40:21.362575Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:36:48.666531Z' and time < '2024-07-17T10:41:48.666531Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:42:22.424993Z' and time < '2024-07-17T10:47:22.424993Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:14:10.857340Z' and time < '2024-07-17T10:19:10.857340Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:37:15.793000Z' and time < '2024-07-17T10:42:15.793000Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:18:44.653670Z' and time < '2024-07-17T10:23:44.653670Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:04:09.768443Z' and time < '2024-07-17T10:09:09.768443Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:43:58.677075Z' and time < '2024-07-17T10:48:58.677075Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:31:02.738328Z' and time < '2024-07-17T10:36:02.738328Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:17:36.591400Z' and time < '2024-07-17T10:22:36.591400Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:14:52.191399Z' and time < '2024-07-17T10:19:52.191399Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:33:39.010149Z' and time < '2024-07-17T10:38:39.010149Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:46:35.502907Z' and time < '2024-07-17T10:51:35.502907Z' ", 
]

total_time1 = 0
count1 = 0

for query in queries1:
    # 构建查询参数
    params = {
        'db': db_name,
        'q': query
    }

    start_time = time.time()

    response = requests.post(url, data=params)

    end_time = time.time()

    # 计算执行时间
    if response.status_code == 200:
        total_time1 += end_time - start_time
        count1 += 1
        print(response.json())


if total_time1 != 0:
    result1 = count1 / total_time1


print(f"result1: {result1:.6f} count1: {count1}")
print("Execution time 1: {:.6f} seconds".format(total_time1))


queries2 = [
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:13:15.886978Z' and time < '2024-07-17T10:43:15.886978Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:08:54.628934Z' and time < '2024-07-17T10:38:54.628934Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:01:36.986839Z' and time < '2024-07-17T10:31:36.986839Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:03:52.141585Z' and time < '2024-07-17T10:33:52.141585Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:07:48.378300Z' and time < '2024-07-17T10:37:48.378300Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:09:51.344210Z' and time < '2024-07-17T10:39:51.344210Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:24:13.840310Z' and time < '2024-07-17T10:54:13.840310Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:06:52.939530Z' and time < '2024-07-17T10:36:52.939530Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:03:56.081770Z' and time < '2024-07-17T10:33:56.081770Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:20:27.670771Z' and time < '2024-07-17T10:50:27.670771Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:11:19.709073Z' and time < '2024-07-17T10:41:19.709073Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:25:24.158305Z' and time < '2024-07-17T10:55:24.158305Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:01:54.696680Z' and time < '2024-07-17T10:31:54.696680Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:16:00.383458Z' and time < '2024-07-17T10:46:00.383458Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:16:13.940523Z' and time < '2024-07-17T10:46:13.940523Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:09:21.089578Z' and time < '2024-07-17T10:39:21.089578Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:20:05.293768Z' and time < '2024-07-17T10:50:05.293768Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:09:57.870099Z' and time < '2024-07-17T10:39:57.870099Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:07:14.719353Z' and time < '2024-07-17T10:37:14.719353Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:16:56.882063Z' and time < '2024-07-17T10:46:56.882063Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:12:46.216413Z' and time < '2024-07-17T10:42:46.216413Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:21:15.270827Z' and time < '2024-07-17T10:51:15.270827Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:09:33.479919Z' and time < '2024-07-17T10:39:33.479919Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:04:39.675215Z' and time < '2024-07-17T10:34:39.675215Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:00:13.004945Z' and time < '2024-07-17T10:30:13.004945Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:15:11.782513Z' and time < '2024-07-17T10:45:11.782513Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:15:29.740377Z' and time < '2024-07-17T10:45:29.740377Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:00:43.712482Z' and time < '2024-07-17T10:30:43.712482Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:01:55.555533Z' and time < '2024-07-17T10:31:55.555533Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:08:11.286345Z' and time < '2024-07-17T10:38:11.286345Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:02:27.655286Z' and time < '2024-07-17T10:32:27.655286Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:09:03.555584Z' and time < '2024-07-17T10:39:03.555584Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:02:34.246877Z' and time < '2024-07-17T10:32:34.246877Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:17:03.827241Z' and time < '2024-07-17T10:47:03.827241Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:22:12.162561Z' and time < '2024-07-17T10:52:12.162561Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:23:30.181820Z' and time < '2024-07-17T10:53:30.181820Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:11:34.151239Z' and time < '2024-07-17T10:41:34.151239Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:21:16.130944Z' and time < '2024-07-17T10:51:16.130944Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:10:48.734278Z' and time < '2024-07-17T10:40:48.734278Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:25:00.034244Z' and time < '2024-07-17T10:55:00.034244Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:04:20.433442Z' and time < '2024-07-17T10:34:20.433442Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:23:22.989076Z' and time < '2024-07-17T10:53:22.989076Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:19:45.896819Z' and time < '2024-07-17T10:49:45.896819Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:23:11.412430Z' and time < '2024-07-17T10:53:11.412430Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:02:45.206753Z' and time < '2024-07-17T10:32:45.206753Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:04:22.849544Z' and time < '2024-07-17T10:34:22.849544Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:16:27.794661Z' and time < '2024-07-17T10:46:27.794661Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:07:17.992019Z' and time < '2024-07-17T10:37:17.992019Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:16:04.538209Z' and time < '2024-07-17T10:46:04.538209Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:16:03.446440Z' and time < '2024-07-17T10:46:03.446440Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:01:56.402197Z' and time < '2024-07-17T10:31:56.402197Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:06:38.864653Z' and time < '2024-07-17T10:36:38.864653Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:04:20.066411Z' and time < '2024-07-17T10:34:20.066411Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:17:27.410485Z' and time < '2024-07-17T10:47:27.410485Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:13:23.480456Z' and time < '2024-07-17T10:43:23.480456Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:14:34.953109Z' and time < '2024-07-17T10:44:34.953109Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:19:56.428501Z' and time < '2024-07-17T10:49:56.428501Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:12:09.503650Z' and time < '2024-07-17T10:42:09.503650Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:08:53.217527Z' and time < '2024-07-17T10:38:53.217527Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:18:02.707763Z' and time < '2024-07-17T10:48:02.707763Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:23:10.990965Z' and time < '2024-07-17T10:53:10.990965Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:23:25.371626Z' and time < '2024-07-17T10:53:25.371626Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:08:09.221443Z' and time < '2024-07-17T10:38:09.221443Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:03:38.680569Z' and time < '2024-07-17T10:33:38.680569Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:10:04.543309Z' and time < '2024-07-17T10:40:04.543309Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:15:56.409701Z' and time < '2024-07-17T10:45:56.409701Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:02:51.359541Z' and time < '2024-07-17T10:32:51.359541Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:03:13.631088Z' and time < '2024-07-17T10:33:13.631088Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:05:53.784574Z' and time < '2024-07-17T10:35:53.784574Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:20:42.558986Z' and time < '2024-07-17T10:50:42.558986Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:14:58.676733Z' and time < '2024-07-17T10:44:58.676733Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:03:15.704459Z' and time < '2024-07-17T10:33:15.704459Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:10:52.923070Z' and time < '2024-07-17T10:40:52.923070Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:03:39.774075Z' and time < '2024-07-17T10:33:39.774075Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:10:56.113197Z' and time < '2024-07-17T10:40:56.113197Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:15:36.320383Z' and time < '2024-07-17T10:45:36.320383Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:02:26.568700Z' and time < '2024-07-17T10:32:26.568700Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:23:39.040437Z' and time < '2024-07-17T10:53:39.040437Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:15:41.778932Z' and time < '2024-07-17T10:45:41.778932Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:11:02.277622Z' and time < '2024-07-17T10:41:02.277622Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:04:29.450321Z' and time < '2024-07-17T10:34:29.450321Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:14:14.888516Z' and time < '2024-07-17T10:44:14.888516Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:24:59.820079Z' and time < '2024-07-17T10:54:59.820079Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:16:31.365597Z' and time < '2024-07-17T10:46:31.365597Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:10:20.391980Z' and time < '2024-07-17T10:40:20.391980Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:23:15.676039Z' and time < '2024-07-17T10:53:15.676039Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'SF') and time >= '2024-07-17T10:23:44.823638Z' and time < '2024-07-17T10:53:44.823638Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'CHI') and time >= '2024-07-17T10:04:38.951973Z' and time < '2024-07-17T10:34:38.951973Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '0') and time >= '2024-07-17T10:01:48.274735Z' and time < '2024-07-17T10:31:48.274735Z' ",
    "SELECT max(usage_user) from graphstream where (service_version = '1') and time >= '2024-07-17T10:10:08.644622Z' and time < '2024-07-17T10:40:08.644622Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x86') and time >= '2024-07-17T10:02:15.121641Z' and time < '2024-07-17T10:32:15.121641Z' ",
    "SELECT max(usage_user) from graphstream where (arch = 'x64') and time >= '2024-07-17T10:00:30.589551Z' and time < '2024-07-17T10:30:30.589551Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'production') and time >= '2024-07-17T10:04:42.345669Z' and time < '2024-07-17T10:34:42.345669Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'test') and time >= '2024-07-17T10:25:27.987554Z' and time < '2024-07-17T10:55:27.987554Z' ",
    "SELECT max(usage_user) from graphstream where (service_environment = 'staging') and time >= '2024-07-17T10:24:59.619688Z' and time < '2024-07-17T10:54:59.619688Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.10') and time >= '2024-07-17T10:24:53.282151Z' and time < '2024-07-17T10:54:53.282151Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu15.10') and time >= '2024-07-17T10:01:39.681920Z' and time < '2024-07-17T10:31:39.681920Z' ",
    "SELECT max(usage_user) from graphstream where (os = 'Ubuntu16.04LTS') and time >= '2024-07-17T10:10:10.525378Z' and time < '2024-07-17T10:40:10.525378Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'LON') and time >= '2024-07-17T10:01:22.817913Z' and time < '2024-07-17T10:31:22.817913Z' ",
    "SELECT max(usage_user) from graphstream where (team = 'NYC') and time >= '2024-07-17T10:24:25.836314Z' and time < '2024-07-17T10:54:25.836314Z' ",
]

total_time2 = 0
count2 = 0

for query in queries2:
    # 构建查询参数
    params = {
        'db': db_name,
        'q': query
    }

    start_time = time.time()

    response = requests.post(url, data=params)

    end_time = time.time()

    # 计算执行时间
    if response.status_code == 200:
        total_time2 += end_time - start_time
        count2 += 1
        print(response.json())



if total_time2 != 0:
    result2 = count2 / total_time2


print(f"result2: {result2:.6f} count2: {count2}")
print("Execution time 2: {:.6f} seconds".format(total_time2))