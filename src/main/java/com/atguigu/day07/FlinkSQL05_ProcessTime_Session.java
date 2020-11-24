package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
处理时间:会话窗口
会话窗口没有计数窗口
 */
public class FlinkSQL05_ProcessTime_Session {
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
        // a.按照时间会话
//        Table result = table.window(Session.withGap("5.seconds").on("pt").as("tw"))
//                .groupBy("tw,id")
//                .select("id,id.count,tw.end,tw.start");

        // 5.SQL
        // 0.注册表
        tableEnv.createTemporaryView("sensor",table);
        // a.按照时间会话
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) as ct ,session_start(pt,interval '5' second)," +
                "session_end(pt,interval '5' second) " +
                "from sensor group by id,session(pt,interval '5' second)");


        // 5.转换为流进行输出
//        tableEnv.toAppendStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");


        // 6.执行
        env.execute();

    }
}
