package com.atguigu.homework;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author chenhuiup
 * @create 2020-11-20 8:33
 */
/*
3.读取Kafka test1主题的数据计算WordCount存入MySQL.
 */
public class Test04 {
    public static void main(String[] args) {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从kafka中读取数据
        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop102:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "uzi");

        DataStreamSource<String> kafkaDS = env.addSource(new FlinkKafkaConsumer011<String>(
                "flink",
                new SimpleStringSchema(),
                props
        ));

        // 3.读取一行数据，切割转变为（word ， 1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOne = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(new Tuple2<>(s2, 1));
                }
            }
        });

        // 4.将结果写入mysql中
        wordToOne.addSink(new WordToMySql());

        // 5.执行任务
        try {
            env.execute("Test04");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class WordToMySql extends RichSinkFunction<Tuple2<String, Integer>> {
        // 定义连接
        Connection connection;
        PreparedStatement preparedStatement;

        @Override
        public void open(Configuration parameters) throws Exception {
            // 注册驱动
            connection = DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test", "root", "123456");
            // 预编译SQL
            preparedStatement = connection.prepareStatement("INSERT INTO test01(word,cnt) values(?,?) on duplicate key update cnt=cnt+?");
        }

        @Override
        public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
            preparedStatement.setString(1, value.f0);
            preparedStatement.setInt(2, value.f1);
            preparedStatement.setInt(3, value.f1);
            // 执行预编译sql
            preparedStatement.execute();
        }

        @Override
        public void close() throws Exception {
            // 关闭连接
            if (preparedStatement !=null){
                try {
                    preparedStatement.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
            if (connection !=null){
                try {
                    connection.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
