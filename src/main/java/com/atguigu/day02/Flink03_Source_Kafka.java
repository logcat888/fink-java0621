package com.atguigu.day02;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * @author chenhuiup
 * @create 2020-11-17 11:53
 */
/*
从kafka中读取数据
 */
public class Flink03_Source_Kafka {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置全局并行度
        env.setParallelism(1);

        // 全局禁用任务链
//        env.disableOperatorChaining();

        // 2.从kafka读取数据
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "uzi");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>(
                "Flink0621", //设置kafka主题
                new SimpleStringSchema(), //设置反序列化
                props //设置kafka配置信息
        ));

        // 3.原样变换,mapDS 禁用任务链
        SingleOutputStreamOperator<String> mapDS = kafkaDS.map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s;
            }
        }).setParallelism(2) //设置单个算子的并行度
                .disableChaining(); //单个算子禁用任务链

        // 4.原样变换,mapDS, 加入共享组
        SingleOutputStreamOperator<String> shareGroupDS = mapDS
                .map((MapFunction<String, String>) s -> s)
                .slotSharingGroup("1"); //加入共享组

        // 3.打印数据
        kafkaDS.print("kafka消息");

        // 4.执行任务
        env.execute("Flink03_Source_Kafka");
    }
}
