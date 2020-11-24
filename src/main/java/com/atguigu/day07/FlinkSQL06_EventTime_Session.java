package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Session;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 15:42
 */
/*
事件时间会话窗口:
会话窗口关闭窗口的条件是大于超时时间
 */
public class FlinkSQL06_EventTime_Session {
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
        // a.时间时间语义下会话窗口
        Table result = table.window(Session.withGap("5.second").on("rt").as("tt"))
                .groupBy("tt,id")
                .select("id,id.count,tt.start,tt.end");

        // 5.SQL
        // a.注册表
        tableEnv.createTemporaryView("sensor",table);

        // b.时间时间语义下会话
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) as ct,Session_start(rt,interval '5' second)" +
                ",Session_end(rt,interval '5' second)" +
                " from sensor group by id,Session(rt,interval '5' second)");

        // 6.打印
        tableEnv.toAppendStream(result, Row.class).print("result");
//        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        // 7.执行任务
        env.execute();
    }
}
