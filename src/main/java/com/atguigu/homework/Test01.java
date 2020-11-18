package com.atguigu.homework;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-18 8:34
 */
/*
使用"Reduce"来实现Flink中流式WordCount。
 */
public class Test01 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从文本读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input");

        // 3.读取一行数据，切割变换结构为二元组（word 1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = inputDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        });

        // 4分组聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceDS = wordToOneDS
                .keyBy(0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                        return new Tuple2<>(t1.f0, t2.f1 + t1.f1);
                    }
                });

        // 5.打印输出
        reduceDS.print("word count");
        // 6.执行任务
        env.execute();
    }
}
