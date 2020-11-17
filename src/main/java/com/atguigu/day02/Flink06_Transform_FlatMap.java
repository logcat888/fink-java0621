package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-17 16:02
 */
public class Flink06_Transform_FlatMap {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从自定义source源中读取数据
        DataStreamSource<String> lineDS = env.readTextFile("sensor");

        // 3.将每一行数据转换为JavaBean
        SingleOutputStreamOperator<String> flatMapDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(",");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        // 4.打印
        flatMapDS.print();

        // 5.执行任务
        env.execute("Flink06_Transform_FlatMap");
    }
}
