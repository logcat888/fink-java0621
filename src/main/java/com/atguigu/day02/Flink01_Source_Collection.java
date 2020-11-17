package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author chenhuiup
 * @create 2020-11-17 11:52
 */
/*
从集合中创建有界流
 */
public class Flink01_Source_Collection {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        // 2.从集合中创建有界流
        DataStreamSource<SensorReading> sensorDS = env.fromCollection(
                Arrays.asList(
                        new SensorReading("sensor_1", 1547718199L, 35.8),
                        new SensorReading("sensor_6", 1547718201L, 15.4),
                        new SensorReading("sensor_7", 1547718202L, 6.7),
                        new SensorReading("sensor_10", 1547718205L, 38.1)
                )
        );

        // 3.打印
        sensorDS.print();

        // 4.执行任务
        env.execute("Flink01_Source_Collection");
    }
}
