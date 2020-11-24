package com.atguigu.day04;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * @author chenhuiup
 * @create 2020-11-20 17:52
 */
/*
Watermark的传递：注意设置并行度已经为2
1.先提取时间，再经过转换，这时所有的算子的Watermark都是同步的，即只要Watermark到了窗口时间就会触发计算
2.先经过转换map，再提取时间，这时下游并行的任务必须接受到所有上游的任务后才能根据最小的Watermark生成Watermark，
    即只要Watermark到了窗口时间就会触发计算
 */
public class Flink05_Window_EventTime_Watermark_Trans2 {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        //指定使用事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //2.读取端口数据
        //DataStreamSource<String> input = env.readTextFile("input");
        SingleOutputStreamOperator<String> input = env.socketTextStream("hadoop102", 7777);

        //3.将数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> map = input.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            // 提取时间
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        });

        //4.重分区
        KeyedStream<SensorReading, Tuple> keyedStream = map.keyBy("id");

        //5.开窗
        WindowedStream<SensorReading, Tuple, TimeWindow> windowedStream = keyedStream.timeWindow(Time.seconds(5));

        //6.计算
        SingleOutputStreamOperator<SensorReading> result = windowedStream.sum("temp");

        //7.打印
        result.print();

        //8.执行任务
        env.execute();
    }
}
