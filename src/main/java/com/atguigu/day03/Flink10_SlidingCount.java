package com.atguigu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-18 17:49
 */
public class Flink10_SlidingCount {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口中读取数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102",7777);

        //3.读取一行数据，分割为元组类型（word，1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s1 : s.split(" ")) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        });

        //4.分组
//        KeyedStream泛型：输出数据泛型，key的泛型，注意根据元组位置作为key后泛型是Tuple类型
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = wordToOneDS.keyBy(0);

        // 5.开窗,聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyByDS
                .countWindow(10,2)
                .sum(1);

        // 6.打印
        sum.print();

        //7.执行
        env.execute();
    }
}
