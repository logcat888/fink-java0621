package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 15:43
 */
/*
按照事件时间开窗:
1.在hive中如果有 1,2,2,3,4,5几个数，对其进行开窗（按照数字排序），求和。
    row     window          sum
    1       1               1
    2       1,2,2           5
    2       1,2,2           5
    3       1,2,2,3         8
    4       1,2,2,3,4       12
    5       1,2,2,3,4,5     17
  注：开窗排序相同的数据会进入同一个窗口
2. 在时间语义为事件时间时进行开窗，只有watermark推进时才会进行计算，没有推进就不计算。当相同时间的多个数据到来时，
    由于watermark没有推进，所以不会触发计算，只有当新的watermark产生时才会计算。
    比如现在watermark为190，这时到来数据是（a,190）（b,190）（a,190） ，只有来了一条(a,191)数据时才会计算。
 */
public class FlinkSQL08_EventTime_Over {
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
        // a.事件时间语义下开窗
//        Table result = table.window(Over.partitionBy("id").orderBy("rt").as("tt"))
//                .select("id,id.count over tt");

        // 5.SQL
        // a.注册表
        tableEnv.createTemporaryView("sensor",table);

        // b.事件时间语义下开窗
        Table sqlResult = tableEnv.sqlQuery("select id,count(id) over (partition by id order by rt) from sensor");

        // 6.打印
//        tableEnv.toAppendStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        // 7.执行任务
        env.execute();
    }
}
