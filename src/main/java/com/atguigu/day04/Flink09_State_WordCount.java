package com.atguigu.day04;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-20 17:53
 */
/*
使用process API和状态实现WordCount
 */
public class Flink09_State_WordCount {
    public static void main(String[] args) throws Exception {
        // 1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 2.从端口获取数据
        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 7777);

        // 3.使用Process API实现压平，并转换结构为（word，1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = socketDS.process(new ProcessFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void processElement(String value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] fields = value.split(" ");
                for (String field : fields) {
                    out.collect(new Tuple2<>(field, 1));
                }
            }
        });

        // 4.分组
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedDS = wordToOneDS.keyBy(0);

        // 5.使用process API实现WordCount
        SingleOutputStreamOperator<Tuple2<String, Integer>> countDS = keyedDS.process(new MySum());

        // 6.打印
        countDS.print("count");

        // 7.执行
        env.execute();
    }

    public static class MySum extends KeyedProcessFunction<Tuple, Tuple2<String, Integer>, Tuple2<String, Integer>> {

        // 定义状态
        ValueState<Integer> count = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class));
//            count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class,0));

        }

        @Override
        public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            //获取状态
            Integer sum = count.value();
            if (sum == null){
                sum =0;
            }
            //写出
            out.collect(new Tuple2<String, Integer>(value.f0, sum + 1));
            // 更新状态
            count.update(sum + 1);
        }
    }
}
