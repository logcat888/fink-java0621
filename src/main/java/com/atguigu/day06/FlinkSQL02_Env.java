package com.atguigu.day06;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:21
 */
/*
执行环境：
1.old planner：1.10默认是old Planner，1.11默认是blink Planner
    1）流处理环境
    2）批处理环境
2.blink planner：
    1）流处理环境
    2）批处理环境

3.批流统一：Blink将批处理作业，视为流式处理的特殊情况。所以，blink不支持表和DataSet之间的转换，
    批处理作业将不转换为DataSet应用程序，而是跟流处理一样，转换为DataStream程序来处理。
4.因为批流统一，Blink planner也不支持BatchTableSource，而使用有界的StreamTableSource代替。

 */
public class FlinkSQL02_Env {
    public static void main(String[] args) {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.基于老版本的流式处理环境
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner() //使用老版本planner
                .inStreamingMode() //流处理模式
                .build();

        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        //3.基于老版本的批处理环境
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment.create(batchEnv);

        //4.基于新版本的流式处理环境
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        //5.基于新版本的批处理环境
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        StreamTableEnvironment blinkBatchTableEnv = StreamTableEnvironment.create(env, blinkBatchSettings);
    }
}
