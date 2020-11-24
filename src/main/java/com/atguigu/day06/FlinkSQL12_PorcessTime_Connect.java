package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:22
 */
/*
处理时间：
使用proctime替换或追加处理时间字段:
1. DataStream转化成Table时指定
2. 定义Table Schema时指定
3. 创建表的DDL中指定(必须使用blink planner运行)
 */
public class FlinkSQL12_PorcessTime_Connect {
    public static void main(String[] args) {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.定义Table Schema时指定
        tableEnv.connect(new FileSystem().path("sensor"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                .field("id", DataTypes.STRING())
                .field("ts",DataTypes.BIGINT())
                .field("temp",DataTypes.DOUBLE())
                .field("pt",DataTypes.TIMESTAMP(3)).proctime())
                .createTemporaryTable("sensor");

        // 4.打印schema
        Table sensor = tableEnv.from("sensor");
        sensor.printSchema();
        /*
        root
         |-- id: STRING
         |-- ts: BIGINT
         |-- temp: DOUBLE
         |-- pt: TIMESTAMP(3)
         */
    }
}
