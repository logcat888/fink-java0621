package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 15:43
 */
/*
处理时间开窗：order by（事件时间/处理时间） 和 as 是必须的,而partition by 和 preceding是可选的，preceding默认是上无边界
由于是按行开窗，所以是追加模式
 */
public class FlinkSQL07_ProcessTime_Over {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.从端口获取数据,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        // 3.将流转换为表,追加处理时间
        Table table = tableEnv.fromDataStream(sensorDS,"id,ts,temp,pt.proctime");

        // 4.Table API
        // a.按照处理时间开窗
        Table result = table.window(Over.partitionBy("id").orderBy("pt").as("tw"))
                .select("id,id.count over tw");

        // 5.SQL
        // 0.注册表
        tableEnv.createTemporaryView("sensor",table);
        // a.按照处理时间开窗
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) over (partition by id order by pt) from sensor");


        // 5.转换为流进行输出
//        tableEnv.toRetractStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");


        // 6.执行
        env.execute();

    }
}
