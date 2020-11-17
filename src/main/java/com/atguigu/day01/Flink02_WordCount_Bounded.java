package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-16 14:46
 */
/*
使用Flink处理有界流： 将批处理的数据当做流式数据处理

 */
public class Flink02_WordCount_Bounded {
    public static void main(String[] args) throws Exception {
        // 1. 创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文本文件读取数据
        DataStreamSource<String> input = env.readTextFile("input");
        // 3. 转变数据结构,由一行数据变换为元组（word，1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new MyFlatMapFunction());
        // 4. 分组：按照word进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);
        // 5. 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = keyedDS.sum(1);
        // 6.
        wordToOneDS.print("wordToOneDS");
        result.print("result");
        // 7.开启任务
        env.execute("Flink02_WordCount_Bounded");
    }
}
