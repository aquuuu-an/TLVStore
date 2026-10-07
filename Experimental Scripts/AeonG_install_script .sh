sudo docker pull hououou/aeong:v1
sudo docker run -dit --entrypoint /bin/bash --cap-add=SYS_PTRACE --network host --security-opt seccomp=unconfined --name aeong --ulimit memlock=-1 -e TZ=Asia/Shanghai hououou/aeong:v1
docker exec -it <container_id> /bin/bash

source /opt/toolchain-v4/activate
cd /home
git clone https://github.com/hououou/AeonG.git
cd AeonG/libs
1. download two files in directory AeonG/libs
 https://github.com/memgraph/memgraph/blob/v2.2.0/libs/antlr4.patch
 https://github.com/memgraph/memgraph/blob/v2.2.0/libs/librdtsc.patch
2. rocksdb lib
2.1 modify setup.sh
Comment out line 199:
git apply ../rocksdb.patch -> //git apply ../rocksdb.patch
2.2 need manually modify AeonG/libs/rocksdb/CMakeLists.txt 
following https://github.com/memgraph/memgraph/blob/v2.2.0/libs/rocksdb.patch
3. export LD_LIBRARY_PATH=/home/AeonG/libs/protobuf/lib:$LD_LIBRARY_PATH
4. git checkout -- quicklisp.lisp
cd build
cmake ..
make -j$(nproc) memgraph