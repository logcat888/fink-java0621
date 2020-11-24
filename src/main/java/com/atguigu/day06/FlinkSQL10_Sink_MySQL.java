package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:22
 */
/*
流式管道：从端口中读取数据，写入到mysql中；
1. 只能使用DDL语言定义连接器：设置mysql的连接参数
2. 无法指定append或upsert。
    1）groupBy字段是主键：就是upsert
    2）groupBy字段不是主键：就是append
 */
public class FlinkSQL10_Sink_MySQL {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.从端口读取数据，并转换为JavaBean对象
        SingleOutputStreamOperator<SensorReading> socketDS = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 4.创建表
        tableEnv.createTemporaryView("sensor", socketDS);

        // 5.执行SQL
        Table table = tableEnv.sqlQuery("select id,count(id) as cnt from sensor group by id");

        // 6.定义mysql的连接器
        String sinkDDL = "create table jdbcOutputTable (" +
                " id varchar(20) not null, " +
                " cnt bigint not null " +
                ") with (" +
                " 'connector.type' = 'jdbc', " +
                " 'connector.url' = 'jdbc:mysql://hadoop102:3306/test', " +
                " 'connector.table' = 'sensor_count2', " +
                " 'connector.driver' = 'com.mysql.jdbc.Driver', " +
                " 'connector.username' = 'root', " +
                " 'connector.password' = '123456', " +
                " 'connector.write.flush.max-rows' = '1')";

        tableEnv.sqlUpdate(sinkDDL);
        tableEnv.insertInto("jdbcOutputTable", table);


        // 8.执行任务
        env.execute();
    }
}
