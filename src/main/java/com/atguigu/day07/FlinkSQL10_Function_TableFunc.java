package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 20:06
 */
/*
自定义表函数：UDTF，可以实现一行进，多行多列出，多列可以用元组类型表示
 */
public class FlinkSQL10_Function_TableFunc {
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
        tableEnv.registerFunction("Split",new Split("_"));

        //4.Table APi
        Table result = table.joinLateral("Split(id) as (word,length)")
                .select("id,word,length");

        //5.SQL
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id,word,length from sensor,lateral table(Split(id)) as T(word,length)");

        //6.打印
//        tableEnv.toAppendStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        //7.执行
        env.execute();


    }

    public static class Split extends TableFunction<Tuple2<String,Integer>>{

        private String operator;

        public Split(String operator){
            this.operator = operator;
        }

        public void eval(String value) {
            String[] split = value.split(operator);
            for (String s : split) {
                collect(new Tuple2<>(s,s.length()));
            }

        }
    }
}
