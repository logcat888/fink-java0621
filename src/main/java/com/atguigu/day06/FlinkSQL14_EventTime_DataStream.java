package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:23
 */
/*
事件时间：
使用rowtime替换或追加处理时间字段:
1. DataStream转化成Table时指定
2. 定义Table Schema时指定
3. 创建表的DDL中指定(必须使用blink planner运行)
 */
public class FlinkSQL14_EventTime_DataStream {
    public static void main(String[] args) {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设定事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3.从端口读取数据，并转换为JavaBean对象,提取时间戳并周期性生成watermark
        SingleOutputStreamOperator<SensorReading> socketDS = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
            @Override
            public long extractTimestamp(SensorReading element) {
                return element.getTs() * 1000L;
            }
        });

        // 4.将流转换为Table
        // 追加
        Table table = tableEnv.fromDataStream(socketDS, "id,ts,temp,rt.rowtime");
        // 5.打印schema
        table.printSchema();
        /*
        root
 |-- id: STRING
 |-- ts: BIGINT
 |-- temp: DOUBLE
 |-- rt: TIMESTAMP(3) *ROWTIME*
         */
    }
}
