package com.atguigu.day05;

import com.atguigu.bean.SensorReading;
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

/**
 * @author chenhuiup
 * @create 2020-11-21 11:19
 */
/*
需求：温度连续10秒钟没有下降，使用处理时间,使用process API的定时器功能
 */
public class Flink02_State_Ontimer {
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

        // 5.使用process API的定时器功能，实现温度连续10秒钟没有下降
        SingleOutputStreamOperator<String> warningDS = sensorDS.process(new MyKeyedProcess());

        // 6. 打印
        warningDS.print("warning");

        // 7.执行任务
        env.execute();
    }

    public static class MyKeyedProcess extends KeyedProcessFunction<Tuple,SensorReading,String>{

        // 定义温度状态，保存上一次的温度
        ValueState<Double> lastTempState;
        // 定义时间状态，保存定时器的时间
        ValueState<Long> timerState;
        @Override
        public void open(Configuration parameters) throws Exception {
            lastTempState  = getRuntimeContext().getState(new ValueStateDescriptor<Double>("lastTemp", Double.class));
            timerState  = getRuntimeContext().getState(new ValueStateDescriptor<Long>("timerState", Long.class));

        }

        @Override
        public void processElement(SensorReading value, Context ctx, Collector<String> out) throws Exception {
            // 获取状态
            Double lastTemp = lastTempState.value();
            Long timer = timerState.value();
            long ts = ctx.timerService().currentProcessingTime() + 10000L;
            if (timer == null){
                // 如果timer为null，说明定时器没有注册,注册定时器10秒
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 更新timer状态
                timerState.update(ts);
            }else if (lastTemp != null && lastTemp>value.getTemp()){
                // 当本次把温度小于上次温度，删除定时器，并注册新的定时器
                ctx.timerService().deleteProcessingTimeTimer(timer);
                ctx.timerService().registerProcessingTimeTimer(ts);
                // 更新timer的状态
                timerState.update(ts);
            }

            // 更新温度
            lastTempState.update(value.getTemp());
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 定时时间到输出报警信息
            out.collect(ctx.getCurrentKey() + "传感器从" + timestamp + "到现在"
                    + ctx.timerService().currentProcessingTime() +"已经连续10秒没有温度降低"
                    + ",定时的时间:" + timerState.value() + ",ctx中的timestamp:" + ctx.timestamp());


            // 清空timer状态,以便注册新的定时器
            timerState.clear();

        }
    }
}
