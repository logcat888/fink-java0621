package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-17 16:03
 */
public class Flink07_Transform_Filter {
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

        // 4.过滤出温度大于30度的数据
        SingleOutputStreamOperator<SensorReading> filterDS = sensorDS.filter(s -> s.getTemp() > 30.0);

        // 5.打印
        filterDS.print();

        // 5.执行任务
        env.execute("Flink07_Transform_Filter");
    }
}
