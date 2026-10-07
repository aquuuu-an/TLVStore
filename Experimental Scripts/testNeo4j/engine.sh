#!/bin/bash
# engine.sh [insert|query] [scale]

USER="neo4j"
PASS="your_password"

if [ "$1" == "insert" ]; then
    SCALE=$2
    # 假设你已经准备好了不同规模的数据文件
    START_TIME=$(date +%s%N)
    cypher-shell -u $USER -p $PASS \
    "LOAD CSV WITH HEADERS FROM 'file:///data_$SCALE.csv' AS line 
     MERGE (n:Person {id: line.id}) SET n.name = line.name;" 
    END_TIME=$(date +%s%N)
    
    # 计算持续时间（毫秒）
    DURATION=$(( (END_TIME - START_TIME) / 1000000 ))
    echo "Insert_Duration_ms: $DURATION" > "./logs/insert_res_$SCALE.txt"

elif [ "$1" == "query" ]; then
    # 执行几类典型查询：1跳、2跳、聚合
    for Q_TYPE in "1_hop" "2_hop" "agg"; do
        START_TIME=$(date +%s%N)
        # 执行查询逻辑
        cypher-shell -u $USER -p $PASS "MATCH (n:Person)-[:FRIEND]->(m) RETURN count(m);" > /dev/null
        END_TIME=$(date +%s%N)
        echo "$Q_TYPE: $(( (END_TIME - START_TIME) / 1000000 )) ms"
    done
fi