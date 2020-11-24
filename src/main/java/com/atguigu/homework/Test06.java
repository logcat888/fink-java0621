package com.atguigu.homework;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

/**
 * @author chenhuiup
 * @create 2020-11-21 8:49
 */
/*
使用事件时间处理数据,编写代码从端口获取数据实现每隔5秒钟计算最近30秒的每个传感器发送温度的次数,
Watermark设置延迟2秒钟,允许迟到数据2秒钟,再迟到的数据放置侧输出流
 */
public class Test06 {
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


        // 3.转换为Javabean
        SingleOutputStreamOperator<SensorReading> sensorDS = watermarkDS.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] split = value.split(",");
                return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
            }
        });

        // 4.分组，开窗，设置延迟
        WindowedStream<SensorReading, String, TimeWindow> mainDS = sensorDS.keyBy(new KeySelector<SensorReading, String>() {
            @Override
            public String getKey(SensorReading value) throws Exception {
                return value.getId();
            }
        }).timeWindow(Time.seconds(30), Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(new OutputTag<SensorReading>("late") {
                });

        // 5.聚合每个传感器发送温度的次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> aggregateDS = mainDS.aggregate(new AggregateFunction<SensorReading, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> createAccumulator() {
                return new Tuple2<>("", 0);
            }

            @Override
            public Tuple2<String, Integer> add(SensorReading value, Tuple2<String, Integer> accumulator) {
                return new Tuple2<>(value.getId(), accumulator.f1 + 1);
            }

            @Override
            public Tuple2<String, Integer> getResult(Tuple2<String, Integer> accumulator) {
                return new Tuple2<>(accumulator.f0, accumulator.f1);
            }

            @Override
            public Tuple2<String, Integer> merge(Tuple2<String, Integer> a, Tuple2<String, Integer> b) {
                return new Tuple2<>(a.f0, a.f1 + b.f1);
            }
        });

        // 6.获取侧输出流
        DataStream<SensorReading> sideOutputDS = aggregateDS.getSideOutput(new OutputTag<SensorReading>("late") {
        });

        // 7.打印输出
        aggregateDS.print("main");

        // 8.执行任务
        env.execute();
    }
}
