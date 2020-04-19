#### 环境说明
Flink 1.10
Kafka 0.10.2
#### 配置
环境配置文件在 src/main/resources/config 目录
application-local.properties 本地环境
application-test.properties 测试环境
application-prod.properties 生成环境

#### 打包
mvn clean package -Pproduct  打生产环境包
mvn clean package -Ptest  打测试环境包
mvn clean package -Plocal  打本地包

-Dmaven.test.skip=true 跳过测试用例

hdfs dfs -ls hdfs://tal-cluster/user/flink/checkpoints/09e18819c0a3e5e6171c67692eb4b650
 /usr/local/Cellar/apache-flink/1.10.0/libexec/bin/start-cluster.sh
 
#### 启动
flink run \
-c com.yineng.stream.Streaming target/flink-learning-1.0-SNAPSHOT.jar

### 恢复
flink run \
-s path \
-c com.yineng.stream.Streaming target/flink-learning-1.0-SNAPSHOT.jar \
