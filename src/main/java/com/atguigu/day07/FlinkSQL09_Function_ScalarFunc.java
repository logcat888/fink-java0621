package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 20:05
 */
/*
标量聚合函数：UDF
1. 自定义函数继承ScalarFunction
2. 必须定义eval的方法，以便反射时调用
3. 注册函数
4. 使用函数跟系统内置函数一致
 */
public class FlinkSQL09_Function_ScalarFunc {
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

        //4.注册函数
        tableEnv.registerFunction("MyLength",new MyLength());

        //5.Table API
        Table result = table.select("id,id.MyLength");

        //6.sql
        // 注册表
        tableEnv.createTemporaryView("sensor",table);
        Table sqlResult = tableEnv.sqlQuery("select id ,MyLength(id) from sensor");

        //7.打印
//        tableEnv.toAppendStream(result, Row.class).print("result");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        //8.执行任务
        env.execute();
    }

    // 定义一个UDF函数实现求字符串的长度
    public static class MyLength extends ScalarFunction{

        public int eval(String value){
            return value.length();
        }
    }
}
