package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

/**
 * @author chenhuiup
 * @create 2020-11-20 17:52
 */
/*
设置窗口延迟
 */
public class Flink02_Window_EventTime_Late {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 指定使用事件时间，默认使用处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从端口读取数据,并提取时间戳和生成watermark的机制
        // 注：数据格式sensor_1,1547718199,35.8
        SingleOutputStreamOperator<String> socketDS = env.socketTextStream("hadoop102", 7777)
                // 允许2秒的Watermark延迟
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)){

                    @Override
                    public long extractTimestamp(String element) {
                        return Long.parseLong(element.split(",")[1]) * 1000L;
                    }
                });

        // 3.压平
        SingleOutputStreamOperator<org.apache.flink.api.java.tuple.Tuple2<String, Integer>> sensorToOneDS = socketDS.map(new MapFunction<String, org.apache.flink.api.java.tuple.Tuple2<String, Integer>>() {
            @Override
            public org.apache.flink.api.java.tuple.Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new org.apache.flink.api.java.tuple.Tuple2<>(split[0], 1);
            }
        });

        // 4.分组，开窗，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = sensorToOneDS.keyBy(0)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<Tuple2<String, Integer>>("late"){})
                .sum(1);

        // 5.打印
        sum.print("main");

        // 6.获取侧输出流
        sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("late"){}).print("side Output");


        // 6.执行任务
        env.execute("Flink01_Window_EventTime");
    }
}
