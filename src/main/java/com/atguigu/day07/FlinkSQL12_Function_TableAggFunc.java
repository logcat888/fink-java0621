package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableAggregateFunction;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-24 20:06
 */
/*
表聚合函数：flatAggregate
实现求topN，sql方式无法使用表聚合函数，但是她不需要，他有rank。
1. 自定义类继承TableAggregateFunction
2. 定义两个方法
    public void accumulate(Tuple2<Double, Double> buffer, Double value) {}
     public void emitValue(Tuple2<Double, Double> buffer, Collector<Tuple2<Double, Integer>> collector) {}
3. 注册函数
4. 使用函数：group by 聚合-> flatAggregate 炸裂成表 -> select
5. 由于是聚合结果所以需要使用toRetractStream输出
 */
public class FlinkSQL12_Function_TableAggFunc {
    public static void main(String[] args) throws Exception {
        //1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //2.读取端口数据创建流,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        //3.将流转换为表
        Table table = tableEnv.fromDataStream(sensorDS);

        // 4.注册函数
        tableEnv.registerFunction("Top2Temp", new Top2Temp());

        //4.Table APi
        Table result = table
                .groupBy("id")
                .flatAggregate("Top2Temp(temp) as (temp,rank)")
                .select("id,temp,rank");

        //5.SQL,无法表示表聚合函数

        //6.打印
        tableEnv.toRetractStream(result, Row.class).print("result");

        //7.执行
        env.execute();
    }

    // TableAggregateFunction<T, ACC> T为输出结果，使用二元组表示两列（温度，排名），ACC为中间聚合状态使用二元组表示（第一名，第二名）
    public static class Top2Temp extends TableAggregateFunction<Tuple2<Double, Integer>, Tuple2<Double, Double>> {

        // 初始化缓存区
        @Override
        public Tuple2<Double, Double> createAccumulator() {
            // 设置初始化的值为无法达到的最小值
            return new Tuple2<>(Double.MIN_VALUE, Double.MIN_VALUE);
        }

        // 自定义计算方法
        // 分别为聚合状态，输入数据
        public void accumulate(Tuple2<Double, Double> buffer, Double value) {
            if (value > buffer.f0) {
                //将输入数据跟第一个比较
                buffer.f1 = buffer.f0;
                buffer.f0 = value;
            } else if (value > buffer.f1) {
                //将输入数据跟第二个比较
                buffer.f1 = value;
            }
        }

        // 自定义发射方法
        public void emitValue(Tuple2<Double, Double> buffer, Collector<Tuple2<Double, Integer>> collector) {
            // 发射数据
            collector.collect(new Tuple2<>(buffer.f0, 1));
            if (buffer.f1 != Double.MIN_VALUE) {
                //发射数据
                collector.collect(new Tuple2<>(buffer.f1, 2));
            }
        }

    }
}
