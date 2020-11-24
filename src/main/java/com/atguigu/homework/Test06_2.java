package com.atguigu.homework;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author chenhuiup
 * @create 2020-11-21 9:10
 */
/*
使用事件时间处理数据,编写代码从端口获取数据实现每隔5秒钟计算最近30秒的每个传感器发送温度的次数,
Watermark设置延迟2秒钟,允许迟到数据2秒钟,再迟到的数据放置侧输出流
 */
public class Test06_2 {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //设置事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // 3.提取时间戳,周期性生成watermark
        SingleOutputStreamOperator<String> watermarkDS = socketDS
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(String element) {
                        return Long.parseLong(element.split(",")[1]) * 1000L;
                    }
                });


        // 3.转换为元组，（sensor，1）
        SingleOutputStreamOperator<Tuple2<String,Integer>> sensorDS = watermarkDS.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });

        // 4.分组，开窗，设置延迟,聚合每个传感器发送温度的次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumDS = sensorDS.keyBy(0)
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("late") {
                })
                .sum(1);


        // 5.获取侧输出流
        DataStream<Tuple2<String, Integer>> sideOutputDS = sumDS.getSideOutput(new OutputTag<Tuple2<String, Integer>>("late") {
        });

        // 6.打印输出
        sumDS.print("window");
        sideOutputDS.print("sideOutputDS");

        // 7.执行任务
        env.execute();
    }
}
