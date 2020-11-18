package com.atguigu.homework;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-18 8:35
 */
/*
读取文本数据,将每一行数据拆分成单个单词,对单词进行去重输出。
 */
public class Test03 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);

        // 2.从文本读取数据
        DataStreamSource<String> inputDS = env.readTextFile("input");

        // 3.读取一行数据，切割变换结构压平
        SingleOutputStreamOperator<String> wordDS = inputDS.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] s1 = s.split(" ");
                for (String s2 : s1) {
                    collector.collect(s2);
                }
            }
        });

        SingleOutputStreamOperator<String> mapDS = wordDS.keyBy(new KeySelector<String, String>() {
            @Override
            public String getKey(String s) throws Exception {
                return s;
            }
        }).flatMap(new MyRichFlatMapFunction());

        // 5.打印输出
        mapDS.print("word count");
        // 6.执行任务
        env.execute();
    }
}

class MyRichFlatMapFunction extends RichFlatMapFunction<String,String> {

    MapState<String, Integer> mapState = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        mapState = getRuntimeContext().getMapState(new MapStateDescriptor<String, Integer>("mapState", String.class, Integer.class));
    }

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        if (!mapState.contains(s)){
            collector.collect(s);
            mapState.put(s,1);
        }

    }
}
