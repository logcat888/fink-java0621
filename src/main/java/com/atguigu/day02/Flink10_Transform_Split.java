package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;

/**
 * @author chenhuiup
 * @create 2020-11-17 16:04
 */
/*
Split:可以将一条流打上不同的标签
select：支持选择多个流
 */
public class Flink10_Transform_Split {
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
        SplitStream<SensorReading> splitDS = sensorDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                ArrayList<String> list = new ArrayList<>();
                if (value.getTemp() > 30) {
                    list.add("high");
                } else {
                    list.add("low");
                }
                return list;
            }
        });

        // 5.选择温度流
        DataStream<SensorReading> high = splitDS.select("high");
        DataStream<SensorReading> low = splitDS.select("low");
        DataStream<SensorReading> all = splitDS.select("high","low");


        // 6.打印
        high.print("high");
        low.print("low");
        all.print("all");

        // 7.执行任务
        env.execute("Flink10_Transform_Split");
    }
}
