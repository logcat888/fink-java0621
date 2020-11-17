package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Collections;

/**
 * @author chenhuiup
 * @create 2020-11-17 16:04
 */
/*
Union:合流
可以将多流类型相同的流合并在一条流中
 */
public class Flink12_Transform_Union {
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

        // 4.分流
        SplitStream<SensorReading> splitDS = sensorDS.split(new OutputSelector<SensorReading>() {
            @Override
            public Iterable<String> select(SensorReading value) {
                return value.getTemp() >30 ? Collections.singletonList("high") :Collections.singletonList("low") ;
            }
        });

        // 5.选择温度流
        DataStream<SensorReading> high = splitDS.select("high");
        DataStream<SensorReading> low = splitDS.select("low");

        // 6.合流
        DataStream<SensorReading> union = high.union(low);

        // 7.打印输出
        union.print();

        // 7.执行任务
        env.execute("Flink12_Transform_Union");
    }
}
