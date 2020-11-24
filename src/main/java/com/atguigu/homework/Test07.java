package com.atguigu.homework;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.*;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author chenhuiup
 * @create 2020-11-24 8:35
 */
/*
输入数据如下:
hello,atguigu,hello
hello,spark
hello,flink

使用FlinkSQL实现从Kafka读取数据计算WordCount并将数据写入ES中
 */
public class Test07 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境和表执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.从kafka中读取数据
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "uzi");
        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>("flink",
                new SimpleStringSchema(),
                props));
        SingleOutputStreamOperator<String> wordDS = kafkaDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });

        // 3.定义kafka的连接器
//        tableEnv.connect(new Kafka()
//                .topic("flink")
//                .version("0.11")
//                .property(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092")
//                .property(ConsumerConfig.GROUP_ID_CONFIG, "uzi"))
//                .withFormat(new Csv())
//                .withSchema(new Schema()
//                .field("word", DataTypes.STRING()))
//                .createTemporaryTable("kafkaInput");

        // 4. 将流转换为表
        tableEnv.createTemporaryView("wordFromKafka",wordDS,"word");

        // 5. sql
        Table sqlQuery = tableEnv.sqlQuery("select word,count(word) from wordFromKafka group by word");

        // 6.定义ES的连接器
        tableEnv.connect(new Elasticsearch()
        .version("6")
        .index("wordcount")
        .documentType("_doc")
        .bulkFlushMaxActions(1) //指定每来一条数据写出一次
        .host("hadoop102",9200,"http"))
                .inUpsertMode() // 指定追加模式
                .withFormat(new Json())
                .withSchema(new Schema()
                .field("word",DataTypes.STRING())
                .field("count",DataTypes.BIGINT()))
                .createTemporaryTable("EsOutput");

        // 7.将数据写入到Es
        sqlQuery.insertInto("EsOutput");

        // 8.执行任务
        env.execute();

    }
}
