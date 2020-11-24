package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 15:42
 */
/*
事件时间：滑动窗口
 */
public class FlinkSQL04_EventTime_Slide {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从端口获取数据,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTs() * 1000L;
                    }
                });

        // 3.将流转换为表指定事件时间
        Table table = tableEnv.fromDataStream(sensorDS, "id,ts,temp,rt.rowtime");

        // 4.Table API
        // a.时间时间语义下只能指定时间窗口，不能指定计数窗口
//        Table result = table.window(Slide.over("6.second").every("2.second").on("rt").as("tt"))
//                .groupBy("tt,id")
//                .select("id,id.count,tt.start,tt.end");

        // 5.SQL
        // a.注册表
        tableEnv.createTemporaryView("sensor",table);

        // b.时间时间语义下只能指定时间窗口，不能指定计数窗口
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) as ct,hop_start(rt,interval '6' second,interval '2' second)" +
                ",hop_end(rt,interval '6' second,interval '2' second)" +
                " from sensor group by id,hop(rt,interval '6' second,interval '2' second)");

        // 6.打印
//        tableEnv.toAppendStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        // 7.执行任务
        env.execute();
    }
}
