package com.atguigu.day04;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @author chenhuiup
 * @create 2020-11-20 17:52
 */
/*
Process API:
1. 元组类型keyBy之后类型是Tuple类型;
2. 定义状态必须在open方法中赋值，因为使用到的getRuntimeContext()只有在运行时才会被创建，而如果定义在属性外面，在加载类时就会进行赋值，
    就会出现无法初始化。
3. Process API可以做的事？
    1）状态编程相关；
    2）获取当前的Key以及时间戳；
    3）定时服务相关；
    4）侧输出流
4. 定时器:
    1)注册定时器
    2）在onTimer方法中编辑定时时间到的逻辑
 */
public class Flink06_ProcessAPI_KeyedProcessFunc {
    public static void main(String[] args) {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //2.读取端口数据创建流
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);

        //3.将每一行数据转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = socketTextStream.map(new MapFunction<String, SensorReading>() {
            @Override
            public SensorReading map(String value) throws Exception {
                String[] fields = value.split(",");
                return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
            }
        });

        //4.分组
        KeyedStream<SensorReading, Tuple> keyedStream = sensorDS.keyBy("id");

        //5.使用ProcessAPI处理数据
        keyedStream.process(new MyKeyedProcessFunc());
    }

    /**
     * KeyedProcessFunction<K, I, O> 泛型 Key的类型，输入 ，输出类型
     */
    public static class MyKeyedProcessFunc extends KeyedProcessFunction<Tuple,SensorReading,String>{

        //定义状态
        ValueState<Integer> valueState ;
        @Override
        public void open(Configuration parameters) throws Exception {
           valueState = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("valueState", Integer.class));
        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            //状态编程相关
            valueState.value();

            //获取当前的Key以及时间戳
            ctx.getCurrentKey();
            Long timestamp = ctx.timestamp();

            //定时服务相关
            ctx.timerService().registerEventTimeTimer(timestamp + 10*1000L);
            ctx.timerService().deleteEventTimeTimer(timestamp + 10*1000L);

            //侧输出流
            ctx.output(new OutputTag<String>("side output"),value.getId());
        }
    }
}
