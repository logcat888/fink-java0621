package com.atguigu.day01;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-16 14:46
 */
/*
使用Flink处理无界流： 从端口获取数据进行wordcount
1. 设置并行度参数优先级:
 1）代码优先级最高:
 1.1)局部设置并行度
 1.2)全局设置并行度
 2)提交任务时的命令行
 3)默认配置文件
2. socketTextStream并行度只能是1，从kafka源读取数据并行度可以设置为kafka分区数。
 */
public class Flink03_WordCount_unbounded {
    public static void main(String[] args) throws Exception {
        // 1.创建流式执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 设置代码全局并行度
        env.setParallelism(2);

        // 2.1解析程序入口参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String host = parameterTool.get("host");
        int port = parameterTool.getInt("port");

        // 2.从端口读取数据
        DataStreamSource<String> socketDS = env.socketTextStream(host, port);

        // 3.读取一行数据，变换结构为元组类型，并进行分组，聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = socketDS
                .flatMap(new MyFlatMapFunction()).setParallelism(3) //设置代码算子局部算子
                .keyBy(0)
                .sum(1);

        // 4.打印输出
        result.print();
        // 5.执行任务
        env.execute("Flink03_WordCount_unbounded");
    }
}
