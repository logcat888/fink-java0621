package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:23
 */
/*
处理时间：
使用proctime替换或追加处理时间字段:
1. DataStream转化成Table时指定
2. 定义Table Schema时指定
3. 创建表的DDL中指定:(必须使用blink planner运行)
 */
public class FlinkSQL13_PorcessTime_DDL {
    public static void main(String[] args) {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建blink表执行环境
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);

        // 3.创建表的DDL中指定
        tableEnv.sqlUpdate("create table sensor(" +
                "id varchar(255) not null," +
                "ts bigint," +
                "temp double," +
                "pt as proctime())" +
                "with (" +
                "'connector.type' = 'filesystem'," +
                "'connector.path' = 'sensor'," +
                "'format.type' = 'csv')");
        Table sensor = tableEnv.from("sensor");

        // 4.打印schema
        sensor.printSchema();
        /*
        root
 |-- id: VARCHAR(255) NOT NULL
 |-- ts: BIGINT
 |-- temp: DOUBLE
 |-- pt: TIMESTAMP(3) NOT NULL *PROCTIME* AS PROCTIME()

         */
    }
}
