package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-21 11:19
 */
/*
温度跳变，连续两次温度变化10度
 */
public class Flink01_State_Temp {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // 3.转变为JavaBean对象
        SingleOutputStreamOperator<SensorReading> mapDS = socketDS.map(s -> {
            String[] split = s.split(",");
            return new SensorReading(split[0], Long.parseLong(split[1]), Double.parseDouble(split[2]));
        });

        // 4.分组
        KeyedStream<SensorReading, Tuple> sensorDS = mapDS.keyBy("id");

        // 5.连续两次温度变化10度输出报警
        SingleOutputStreamOperator<Tuple3<String, Double, Double>> warning = sensorDS.flatMap(new MyFlatMap());

        // 6.打印
        warning.print();

        // 7.
        env.execute();

    }

    public static class MyFlatMap extends RichFlatMapFunction<SensorReading, Tuple3<String,Double,Double>>{
        //定义状态
        ValueState<Double> lastTempState;

        @Override
        public void open(Configuration parameters) throws Exception {

            lastTempState  = getRuntimeContext().getState(new ValueStateDescriptor<Double>("valueState", Double.class));
        }

        @Override
        public void flatMap(SensorReading value, Collector<Tuple3<String,Double,Double>> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();
            if (Math.abs(lastTemp - value.getTemp())>10){
                out.collect(new Tuple3<>(value.getId(),lastTemp,value.getTemp()));
            }

            // 更新状态
            lastTempState.update(value.getTemp());
        }
    }
}
