package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.consumer.ConsumerConfig;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:21
 */
/*
从kafka读取数据，定义kafka连接器：
1. kafka支持的格式是CSV或JSON，在Flink官网可以查到
 */
public class FlinkSQL04_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.定义文件连接器
        tableEnv.connect(new Kafka()
                .topic("flink")
                .version("0.11")
                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
                .property(ConsumerConfig.GROUP_ID_CONFIG, "uzi2020"))
//                .withFormat(new Csv())
                .withFormat(new Json())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaInput");

        // 3.创建表
        Table table = tableEnv.from("kafkaInput");

        // 4.Table API
        Table tableResult = table.filter("id = 'sensor_1'").select("id,temp");

        // 5.SQL
        Table sqlResult = tableEnv.sqlQuery("select id,temp from kafkaInput where id = 'sensor_1'");

        // 6.打印
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        // 7.执行任务
        env.execute();
    }
}
