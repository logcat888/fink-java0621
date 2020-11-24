package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:21
 */
/*
聚合操作：求每个传感器的个数
1. 可以使用表执行环境从流或Table中创建SQL需要的表
2. 聚合操作需要更新之前的聚合结果，所以输出时需要使用toRetractStream
 */
public class FlinkSQL05_Agg {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取文本数据创建流并转换为JavaBean
        SingleOutputStreamOperator<SensorReading> inputDS = env.readTextFile("sensor/sensor.txt")
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.创建TableAPI FlinkSQL 的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //4.使用Table执行环境将流转换为Table
        Table table = tableEnv.fromDataStream(inputDS, "id,ts,temp");

        //5.TableAPI
        Table tableResult = table.groupBy("id")
                .select("id,id.count as cnt");

        //6.SQL
        // a.注册表
        tableEnv.createTemporaryView("sensor",inputDS);
        // b.执行SQL
        Table sqlQuery = tableEnv.sqlQuery("select id ,count(id) from sensor group by id");

        //7.将结果数据打印
        tableEnv.toRetractStream(tableResult, Row.class).print("tableResult");
//        tableEnv.toRetractStream(sqlQuery, Row.class).print("sqlQuery");

        /*
        tableResult> (true,sensor_1,1)
        tableResult> (true,sensor_6,1)
        tableResult> (true,sensor_7,1)
        tableResult> (true,sensor_10,1)
        tableResult> (false,sensor_1,1)
        tableResult> (true,sensor_1,2)
        tableResult> (false,sensor_1,2)
        tableResult> (true,sensor_1,3)
        tableResult> (false,sensor_1,3)
        tableResult> (true,sensor_1,4)
         */
        //8.执行
        env.execute();
    }
}
