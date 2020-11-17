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
从文件中读取数据
 */
public class Flink02_Source_File {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置并行度为1
        env.setParallelism(1);

        // 2.从集合中创建有界流
        DataStreamSource<String> sensorDS = env.readTextFile("sensor");

        // 3.打印
        sensorDS.print();

        // 4.执行任务
        env.execute("Flink01_Source_Collection");
    }
}
