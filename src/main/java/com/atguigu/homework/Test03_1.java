package com.atguigu.homework;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFilterFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.HashSet;

/**
 * @author chenhuiup
 * @create 2020-11-18 9:16
 */
/*
读取文本数据,将每一行数据拆分成单个单词,对单词进行去重输出。
 */
public class Test03_1 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从文本读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input");

        // 3.读取一行数据，切割变换结构压平
        SingleOutputStreamOperator<String> redisDS = inputDS.flatMap(new MyFlatMapFunctionRedis());

        // 5.打印输出
        redisDS.print("word count");
        // 6.执行任务
        env.execute();
    }
}

class MyFlatMapFunctionRedis extends RichFlatMapFunction<String,String>{

    private Jedis jedis = null;
    @Override
    public void open(Configuration parameters) throws Exception {
        //获取redis连接
        jedis = new Jedis("hadoop102", 6379);
    }

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        String[] s1 = s.split(" ");

        HashSet<String> set = new HashSet<>(Arrays.asList(s1));

        for (String s2 : set) {
            if (jedis.get(s2) == null){
                jedis.set(s2,"word");
                collector.collect(s2);
            }
        }
    }

    @Override
    public void close() throws Exception {
        try {
            jedis.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}