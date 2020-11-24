package com.atguigu.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @author chenhuiup
 * @create 2020-11-20 17:51
 */
/*
1.时间语义：
    1）事件时间：
    2）摄入时间：当数据中没有事件时间，且想用时间就可以使用摄入时间
    3）处理时间：没有迟到数据一说，窗口时间到就关闭窗口，延迟数据需要输出到侧输出流，否则就会丢失数据
2.如何设置事件时间？
    1）声明事件时间，env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
    2）提取时间：
        1.1）周期性生成Watermark（AssignerWithPeriodicWatermarks）：效率高，在生产中使用
            1.1.1）AscendingTimestampExtractor(抽象类)：最大乱序程度为0
            1.1.2）BoundedOutOfOrdernessTimestampExtractor(抽象类)：最大乱序程度可以指定
        1.2）断点式生成Watermark（AssignerWithPunctuatedWatermarks（接口））：效率低，生产中不使用
3.Trigger触发计算的条件？ 两个触发条件可以分开定义
    1）窗口计算：到时间到窗口时间，会将窗口内的数据统一计算，只输出1次
    2）关窗迟到计算：到了窗口时间，但是在窗口允许迟到的时间范围内，每来一个数据计算1次，会输出多次
4.对于迟到数据的3重保障
    1）Watermark可以设置最大乱序程度，Watermark就是系统的时间
    2）窗口可以设定迟到时间允许迟到数据进入窗口内，与窗口内的数据一起进行计算，这时还没有关闭窗口
    3）在窗口迟到时间外的数据，可以输出到侧输出流。但是侧输出流的数据，不会与窗口内数据一起进行计算，只能手动实现处理
5.关于Watermark、Window的杂七杂八
    1）侧输出流OutputTag<T>必须以内部类的形式出现，必须要有{}
    sum.getSideOutput(new OutputTag<Tuple2<String, Integer>>("late"){})
    must always be an anonymous inner class so that Flink can derive a {@link TypeInformation} for the generic type parameter.
    2) 如果窗口允许的迟到数据的时间增大，那就会延长窗口的占用时间，窗口不能释放，占用资源，且实时性下降
    3）在迟到允许范围的数据会与窗口的数据合并计算，但是来一条处理一条数据，而没有迟到的数据是一块输出。
    4）允许迟到数据是来一条计算一条并输出，效率低。而没有迟到的数据是窗口内一起计算，效率高，只输出一条。
    5）Watermark就是系统时间，决定窗口的计算和关闭，当Watermark到了窗口时间，就会触发窗口的计算；
        当Watermark到了窗口允许的延迟时间就会关闭窗口。
    6）窗口的关闭由窗口延迟决定；
    7） Watermark就是流中的数据，当使用断点式生成Watermark，是每来一条数据生成一个Watermark，效率低；而
        周期性生成Watermark是每200ms生成一个Watermark，效率高；
    8）Watermark设置的延迟，相当于延迟了触发计算，可以运行迟到的数据一块进入窗口，进行1次输出。
6.关于Watermark传递？
    Watermark向下游传递是给所有并行度传递，一个slot会接收上游所有的Watermark，并取最小值，记录成该slot的Watermark。
7.演示Watermark传递
    1）先提取时间，再经过转换，这时所有的算子的Watermark都是同步的，即只要Watermark到了窗口时间就会触发计算
    2）先经过转换map，再提取时间，这时下游并行的任务必须接受到所有上游的任务后才能根据最小的Watermark生成Watermark，
    即只要Watermark到了窗口时间就会触发计算。
8.侧输出流与滑动窗口？
    只有没有进入任何窗口的数据才会进入侧输出流。
 */
public class Flink01_Window_EventTime {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 指定使用事件时间，默认使用处理时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        // 2.从端口读取数据,并提取时间戳和生成watermark的机制
        // 注：数据格式sensor_1,1547718199,35.8
        SingleOutputStreamOperator<String> socketDS = env.socketTextStream("hadoop102", 7777)
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<String>() {
                    @Override
                    public long extractAscendingTimestamp(String element) {
                        return Long.parseLong(element.split(",")[1]) * 1000L;
                    }
                });

        // 3.转换
        SingleOutputStreamOperator<Tuple2<String, Integer>> sensorToOneDS = socketDS.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] split = value.split(",");
                return new Tuple2<>(split[0], 1);
            }
        });
        // 4.分组，开窗，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = sensorToOneDS.keyBy(0)
                .timeWindow(Time.seconds(5))
                .sum(1);

        // 5.打印
        result.print();

        // 6.执行任务
        env.execute("Flink01_Window_EventTime");
    }
}
