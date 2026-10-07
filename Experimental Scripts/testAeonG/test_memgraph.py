import mgclient
import socket

def test_connection(host='localhost', port=7687):
    print(f"1. 测试网络连接 {host}:{port}...")
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(3)
    result = sock.connect_ex((host, port))
    if result == 0:
        print(f"   ✓ 端口 {port} 开放")
    else:
        print(f"   ✗ 无法连接到 {host}:{port}")
        return False
    sock.close()
    
    print("2. 尝试建立 Memgraph 连接...")
    try:
        # 尝试不带认证的连接
        conn = mgclient.connect(host=host, port=port)
        print("   ✓ 不带认证连接成功")
    except Exception as e1:
        print(f"   ✗ 不带认证连接失败: {e1}")
        
        try:
            # 尝试带空认证的连接
            conn = mgclient.connect(
                host=host, 
                port=port, 
                username='', 
                password=''
            )
            print("   ✓ 带空认证连接成功")
        except Exception as e2:
            print(f"   ✗ 带空认证也失败: {e2}")
            return False
    
    print("3. 执行测试查询...")
    try:
        cursor = conn.cursor()
        cursor.execute("RETURN 1 AS result")
        row = cursor.fetchone()
        print(f"   ✓ 查询成功，结果: {row[0]}")
        return True
    except Exception as e:
        print(f"   ✗ 查询失败: {e}")
        return False

if __name__ == "__main__":
    # 尝试不同的主机地址
    hosts_to_try = ['localhost', '127.0.0.1', 'host.docker.internal']
    
    for host in hosts_to_try:
        print(f"\n=== 测试 {host}:7687 ===")
        if test_connection(host):
            print(f"\n✅ 成功连接到 {host}:7687")
            break
    else:
        print("\n❌ 所有连接尝试都失败")