package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import scala.Tuple3;

import java.util.ArrayList;

/**
 * @author chenhuiup
 * @create 2020-11-17 16:04
 */
/*
connect:可以连接类型不同的两个流

 */
public class Flink11_Transform_Connect {
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
        //将高温流转变为三元组类型
        SingleOutputStreamOperator<Tuple3<String, Double, String>> high = splitDS.select("high").map(
                new MapFunction<SensorReading, Tuple3<String, Double, String>>() {
                    @Override
                    public Tuple3<String, Double, String> map(SensorReading s) throws Exception {
                        return  new Tuple3<String, Double, String>(s.getId(), s.getTemp(), "high");
                    }
                });
        DataStream<SensorReading> low = splitDS.select("low");

        // 6.合流
        ConnectedStreams<Tuple3<String, Double, String>, SensorReading> connect = high.connect(low);

        // 7.处理流
        SingleOutputStreamOperator<Object> map = connect.map(new CoMapFunction<Tuple3<String, Double, String>, SensorReading, Object>() {
            @Override
            public Object map1(Tuple3<String, Double, String> value) throws Exception {
                // 变成4元组
                return new Tuple4<String, Double, String, String>(value._1(), value._2(), value._3(), "哈哈哈");
            }

            @Override
            public Object map2(SensorReading value) throws Exception {
                // 变成2元组
                return new Tuple3<String, Double, String>(value.getId(), value.getTemp(), "我是低温");
            }
        });


        //8.打印
        map.print("合流");


        // 7.执行任务
        env.execute("Flink11_Transform_Connect");
    }
}
