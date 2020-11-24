package com.atguigu.day06;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:23
 */
/*
使用rowtime替换或追加处理时间字段:
1. DataStream转化成Table时指定
2. 定义Table Schema时指定
3. 创建表的DDL中指定(必须使用blink planner运行)
 */
public class FlinkSQL16_EventTime_DDL {
    public static void main(String[] args) {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设定事件时间语义
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 2.创建blink表执行环境
        EnvironmentSettings build = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,build);


        //2.创建文件连接器
        String sinkDDL = "create table dataTable (" +
                " id varchar(20) not null, " +
                " ts bigint, " +
                " temp double, " +
                " rt AS TO_TIMESTAMP( FROM_UNIXTIME(ts) ), " +
                " watermark for rt as rt - interval '1' second" +
                ") with (" +
                " 'connector.type' = 'filesystem', " +
                " 'connector.path' = 'sensor', " +
                " 'format.type' = 'csv')";
        tableEnv.sqlUpdate(sinkDDL);

        //3.打印schema信息
        Table table = tableEnv.from("dataTable");
        table.printSchema();
        /*
        root
 |-- id: VARCHAR(20) NOT NULL
 |-- ts: BIGINT
 |-- temp: DOUBLE
 |-- rt: TIMESTAMP(3) AS TO_TIMESTAMP(FROM_UNIXTIME(`ts`))
 |-- WATERMARK FOR rt AS `rt` - INTERVAL '1' SECOND
         */
    }
}
