package com.atguigu.day06;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Rowtime;
import org.apache.flink.table.descriptors.Schema;

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
public class FlinkSQL15_EventTime_Connect {
    public static void main(String[] args) {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        // 设定事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 3.定义Table Schema时指定
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE())
                        .field("rt",DataTypes.TIMESTAMP(3)).rowtime(new Rowtime()
                        .timestampsFromField("ts")
                        .watermarksPeriodicBounded(1000)))
                .createTemporaryTable("sensor");

        // 4.打印schema
        Table sensor = tableEnv.from("sensor");
        sensor.printSchema();
        /*
        root
 |-- id: STRING
 |-- ts: BIGINT
 |-- temp: DOUBLE
 |-- rt: TIMESTAMP(3)
         */
    }
}
