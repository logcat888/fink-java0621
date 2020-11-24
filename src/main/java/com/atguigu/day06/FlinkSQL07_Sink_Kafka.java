package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:22
 */
/*
kafka管道:从kafka读取数据，再写回到kafka，相当于可以做数据清洗
1. kafka sink 仅支持追加模式
 */
public class FlinkSQL07_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.定义kafka连接器，读取数据
        tableEnv.connect(new Kafka()
        .topic("flink")
        .version("0.11")
        .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092")
        .property(ConsumerConfig.GROUP_ID_CONFIG,"uzi2020"))
                .withFormat(new Csv())
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
        Table sqlResult = tableEnv.sqlQuery("select id,ts,temp from kafkaInput where id = 'sensor_1'");

        // 6.定义kafka连接器，写出数据
        tableEnv.connect(new Kafka()
                .topic("flinksql")
                .version("0.11")
                .property(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("kafkaOutput");

        // 7.输出到文件系统
        sqlResult.insertInto("kafkaOutput");

        // 8.执行任务
        env.execute();
    }
}
