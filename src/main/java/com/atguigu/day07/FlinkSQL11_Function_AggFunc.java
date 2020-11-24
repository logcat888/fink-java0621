package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 20:06
 */
/*
聚合函数UDAF：多进一出
注意导包：import org.apache.flink.table.functions.AggregateFunction;
1. 继承AggregateFunction
2. 自定义方法public void accumulate(Tuple2<Double, Integer> buffer, Double value) {
3. 注册函数，使用函数
4. 聚合结果必须使用toRetractStream输出
 */
public class FlinkSQL11_Function_AggFunc {
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
        tableEnv.registerFunction("TempAvg",new TempAvg());

        //4.Table APi
        Table result = table.groupBy("id")
                .select("id,TempAvg(temp)");

        //5.SQL
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id ,TempAvg(temp) from sensor group by id");

        //6.打印
        tableEnv.toRetractStream(result, Row.class).print("result");
//        tableEnv.toRetractStream(sqlResult, Row.class).print("sqlResult");

        //7.执行
        env.execute();

    }

    // AggregateFunction<T, ACC> 输出类型，中间聚合状态
    // 求平均值
    public static class TempAvg extends AggregateFunction<Double, Tuple2<Double, Integer>> {


        //获取返回值结果
        @Override
        public Double getValue(Tuple2<Double, Integer> accumulator) {
            return accumulator.f0 / accumulator.f1;
        }

        //初始化缓冲区
        @Override
        public Tuple2<Double, Integer> createAccumulator() {
            return new Tuple2<>(0.0, 0);
        }

        //计算方法
        public void accumulate(Tuple2<Double, Integer> buffer, Double value) {
            buffer.f0 += value;
            buffer.f1 += 1;
        }
    }
}
