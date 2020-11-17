package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-17 18:41
 */
/*
富函数
 */
public class Flink13_Transform_RichMap {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从自定义source源中读取数据
        DataStreamSource<String> lineDS = env.readTextFile("sensor");

        // 3.将每一行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS =
                lineDS.map(new MyRichMapFunction());

        // 4.打印
        sensorDS.print();

        // 5.执行任务
        env.execute("Flink13_Transform_RichMap");
    }

    public static class MyRichMapFunction extends RichMapFunction<String, SensorReading> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open方法被调用");
            super.open(parameters);
        }

        @Override
        public SensorReading map(String s) throws Exception {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        }

        @Override
        public void close() throws Exception {
            System.out.println("close方法被调用");
            super.close();
        }
    }
}
