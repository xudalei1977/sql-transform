[toc]

### SQL Transfrom from some DB Engine(e.g. AnalyticDB PostgreSQL) to AWS Redshift.

#### 1. 程序

1. CommonSQL: 实现从其他数据库到 Redshift 的常用语句的改造
2. CreateTableSQL: 实现从其他数据库到 Redshift 的建表语句的改造

#### 2. 环境 

```markdown
*  Scala 2.12
```

#### 3. 编译 & 运行

##### 3.1 编译

```properties
# 编译 
mvn clean package -Dscope.type=provided 
```

##### 3.2 运行前准备

创建一台 EC2, 我们的代码是用 Scala 编写的，需要部署 Scala 的运行环境。

```properties
wget https://downloads.lightbend.com/scala/2.12.18/scala-2.12.18.tgz
tar xvf scala-2.12.18.tgz
cd scala-2.12.18/
```
将 Scala 的运行目录放到 PATH

```properties
export SCALA_HOME=/home/ec2-user/scala-2.12.18
export PATH=$SCALA_HOME/bin:$PATH
```

1. CommonSQL 的运行前准备，请先将需要转换的 SQL 语句保存到文件里，然后上传到 EC2 的某个目录下
2. CreateTableSQL 的运行前准备，确认从 EC2 到源数据库的链接成功，并拿到链接使用的用户名和密码


##### 3.3 运行程序说明

###### 3.3.1 CreateTableSQL

* 支持参数

```properties
  CreateTableSQL 1.0
  Usage: Scala CreateTableSQL [options]

    -g, --dbEngine <value>      source database engine, e.g. adb_mysql, adb_pg
    -f, --fileName <value>      file name to contain the create table sql
    -h, --hostname <value>      source database hostname
    -p, --portNo <value>        source database port no.
    -d, --database <value>      source database name
    -s, --schema <value>        schema in source database
    -u, --userName <value>      user name to login source database
    -w, --password <value>      password to login source database
```

* 启动样例

```shell
  # 1. -g 指定源数据库的类型
  # 2. -f 生成Redshift的建表语句，保存在哪个文件
  # 3. -h 源数据库的服务器
  # 4. -p 源数据库的端口
  # 5. -d 源数据库的名称
  # 6. -s 源数据库的Schema
  # 7. -u 登录源数据库的用户名
  # 8. -w 登录源数据库的密码
  
  export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
  scala com.aws.analytics.CreateTableSQL \
    -g adb_mysql -f /home/ec2-user/create_table_redshift.sql \
    -h emr-workshop-mysql8.chl9yxs6uftz.us-east-1.rds.amazonaws.com \
    -p 3306 -d dev -u admin -w ******
```

###### 3.3.2 CommonSQL

* 支持参数

```properties
  CommonSQL 1.0
  Usage: Scala CommonSQL [options]

    -g, --dbEngine <value>      source database engine, e.g. adb_mysql, adb_pg
    -r, --directory <value>     directory contain the sql to be transfered
```

* 启动样例

```shell
  # 1. -g 指定源数据库的类型
  # 2. -f 需要转换成Redshift下运行的SQL语句，所存放的目录
  
  export CLASSPATH=./sql-transform-1.0-SNAPSHOT-jar-with-dependencies.jar:./scopt_2.12-4.0.0-RC2.jar
  scala com.aws.analytics.CommonSQL \
    -g adb_mysql -r /home/ec2-user/adb_sql
```