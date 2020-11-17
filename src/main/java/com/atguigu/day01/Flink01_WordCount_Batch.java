package com.atguigu.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author chenhuiup
 * @create 2020-11-16 14:45
 */
/*
批处理:DataSet --WordCount
1. 使用java版的spark注意导包为java版的
2. java的元组为Tuple0-25
 */
public class Flink01_WordCount_Batch {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // 2. 从文本读取文件
        DataSource<String> input = env.readTextFile("input");

        // 3. 读取一行数据，按空格分隔，并转换为java元组(word,1)
        FlatMapOperator<String, Tuple2<String, Integer>> wordToOneDataSet = input.flatMap(new MyFlatMapFunction());

        // 4.按照word进行分组,groupBy参数为二元组的第1个字段
        UnsortedGrouping<Tuple2<String, Integer>> keyWord = wordToOneDataSet.groupBy(0);

        // 5.按照key进行进行聚合
        AggregateOperator<Tuple2<String, Integer>> result = keyWord.sum(1);

        // 6.输出结果
        result.print();
    }

}

// 自定义FlatMapFunction实现类
class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String,Integer>>{

    @Override
    public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
        String[] split = s.split(" ");
        for (String s1 : split) {
            collector.collect(new Tuple2<>(s1,1));
        }
    }
}

