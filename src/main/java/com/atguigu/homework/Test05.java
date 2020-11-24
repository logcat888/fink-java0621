package com.atguigu.homework;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-20 8:34
 */
/*
Flink中窗口操作的分类,编写代码从端口获取数据实现每隔5秒钟计算最近30秒的WordCount.
 */
public class Test05 {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // 3.读取一行数据，变换结构为元组类型（word ， 1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketDS.flatMap((String s, Collector<Tuple2<String, Integer>> collector) -> {
            String[] s1 = s.split(" ");
            for (String s2 : s1) {
                collector.collect(new Tuple2<String, Integer>(s2, 1));
            }
        });

        // 4.分组，开窗实现每隔5秒钟计算最近30秒
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToSumDS = wordToOneDS
                .keyBy(0)
                .timeWindow(Time.seconds(30), Time.seconds(5))
                .sum(1);

        // 5.打印
        wordToSumDS.print("wordToSum");

        // 6.执行任务
        try {
            env.execute("Test05");
        } catch (Exception e) {
            System.out.println("我关闭了");
        }
    }
}
