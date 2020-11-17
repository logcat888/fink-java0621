package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-17 16:04
 */
/*
计算每个传感器的最高温度以及最近的时间
 */
public class Flink09_Transform_Reduce {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从自定义source源中读取数据
        DataStreamSource<String> lineDS = env.readTextFile("sensor");

        // 3.将每一行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = lineDS.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // 4.计算每个传感器的最高温度以及最近的时间
        KeyedStream<SensorReading, String> keyedDS = sensorDS.keyBy(SensorReading::getId);
        SingleOutputStreamOperator<SensorReading> reduceDS = keyedDS.reduce(new ReduceFunction<SensorReading>() {
            @Override
            public SensorReading reduce(SensorReading sensorReading, SensorReading t1) throws Exception {
                return new SensorReading(sensorReading.getId(),
                        t1.getTs(),
                        Math.max(sensorReading.getTemp(),t1.getTemp()));
            }
        });

        // 5.打印
        reduceDS.print("reduceDS");

        // 6.执行任务
        env.execute("Flink09_Transform_Reduce");
    }
}
