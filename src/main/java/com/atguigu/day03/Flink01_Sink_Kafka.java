package com.atguigu.day03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author chenhuiup
 * @create 2020-11-18 14:32
 */
/*
将结果输出的到kafka中
 */
public class Flink01_Sink_Kafka {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文本中读取数据
        DataStreamSource<String> input = env.readTextFile("input");

        //3.将数据写出到kafka
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop102:9092");

        DataStreamSink<String> uzi = input.addSink(new FlinkKafkaProducer011<String>(
                "uzi",
                new SimpleStringSchema(),
                props
        ));

        //4.执行
        env.execute();
    }
}
