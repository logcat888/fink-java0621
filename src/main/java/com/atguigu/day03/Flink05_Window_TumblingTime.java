package com.atguigu.day03;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-18 17:48
 */
/*
时间滚动窗口：从端口读取数据，如果从文本读取数据，由于读取速度过快，没有等到窗口关闭，JVM就关闭了
1.先分组、再开窗、再聚合
2.增量聚合函数：数据来一条计算一条
3.全窗口函数apply，process：将一个窗口的数据收集放在迭代器中，等窗口关闭后再计算，
        占用内存，效率低。应用场景比如求一个窗口前20%的数据。
4.窗口分为
    4.1 时间窗口
        1）滚动窗口
        2）滑动窗口
        3）会话窗口
    4.2 计算窗口
        1）滚动窗口
        2）滑动窗口
5. window是keyBy之后开窗，windowAll（全窗口）不需要在keyBy之后。
6. timeWindow是window的简写形式，一个参数是滚动窗口，两个参数滑动窗口
This is a shortcut for either {@code .window(TumblingEventTimeWindows.of(size))} or
{@code .window(TumblingProcessingTimeWindows.of(size))} depending on the time characteristic set using
7. 窗口的时间范围？ 定义window的起始时间戳必须大于offset偏移量，防止出现负数，导致起始时间大于定义的起始时间而出现丢数据的情况
protected TumblingEventTimeWindows(long size, long offset) {
if (Math.abs(offset) >= size) {throw new IllegalArgumentException(
"TumblingEventTimeWindows parameters must satisfy abs(offset) < size");}

	public static long getWindowStartWithOffset(long timestamp, long offset, long windowSize) {
		return timestamp - (timestamp - offset + windowSize) % windowSize;
	}
8. 设置偏移量，不能使用简写形式，必须使用window，由于相应的窗口私有化构造器，所以使用of方法设置
  window(SlidingProcessingTimeWindows.of(Time.days(1),Time.hours(-8)))
9. 会话 window(EventTimeSessionWindows.withGap(Time.seconds(10)))
10. 全窗口函数apply: WindowFunction<IN, OUT, KEY, W extends Window> W为一个窗口函数
 */
public class Flink05_Window_TumblingTime {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.从端口中读取数据
        DataStreamSource<String> input = env.socketTextStream("hadoop102",7777);

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

        // 5.开窗,聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyByDS
                .timeWindow(Time.seconds(3))
                .sum(1);

        // 6.打印
        sum.print();

        //7.执行
        env.execute();
    }
}
