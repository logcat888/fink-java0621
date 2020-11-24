package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-23 15:25
 */
/*
牛刀小试：
1.从流中创建Table，从流中注册表
2.读取文本文件创建流
 */
public class FlinkSQL01_Test {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.从文本文件中读取数据，创建流，并转换为javaBean对象
        SingleOutputStreamOperator<SensorReading> inputStream = env.readTextFile("sensor/sensor.txt").map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 4.使用table API
        // a.从流中创建Table
        Table table = tableEnv.fromDataStream(inputStream);
        // b.过滤sensor_1
        Table tableResult = table.filter("id = 'sensor_1'").select("id,temp");

        // 5.使用SQL
        // a.注册表
        tableEnv.createTemporaryView("sensor",inputStream);
        // b.过滤sensor_1
        Table sqlQuery = tableEnv.sqlQuery("select id,temp from sensor where id = 'sensor_1'");

        // 6.打印
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlQuery, Row.class).print("sqlQuery");

        // 7.执行任务
        env.execute();
    }
}
