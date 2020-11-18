package com.atguigu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author chenhuiup
 * @create 2020-11-18 17:49
 */
public class Flink11_Window_Apply {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口中读取数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102", 7777);

        //3.读取一行数据，分割为元组类型（word，1）
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordToOneDS = input.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                for (String s1 : s.split(" ")) {
                    collector.collect(new Tuple2<>(s1, 1));
                }
            }
        });

        //4.分组
//        KeyedStream泛型：输出数据泛型，key的泛型，注意根据元组位置作为key后泛型是Tuple类型
        KeyedStream<Tuple2<String, Integer>, Tuple> keyByDS = wordToOneDS.keyBy(0);

        // 5.开窗,全窗口函数，求一个窗口中数据的个数
        SingleOutputStreamOperator<Integer> sum = keyByDS
                .timeWindow(Time.seconds(10))
                .apply(new myWindowFunction());

        // 6.打印
        sum.print();

        //7.执行
        env.execute();
    }

    //   WindowFunction<IN, OUT, KEY, W extends Window>
    public static class myWindowFunction implements WindowFunction<Tuple2<String, Integer>, Integer, Tuple, TimeWindow> {

        @Override
        public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Integer>> input, Collector<Integer> out) throws Exception {
            Iterator<Tuple2<String, Integer>> iterator = input.iterator();
            int sum = 0;
            while (iterator.hasNext()) {
                Tuple2<String, Integer> next = iterator.next();
                sum++;
            }
            out.collect(sum);
        }
    }
}
