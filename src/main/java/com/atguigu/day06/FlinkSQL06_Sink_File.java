package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:22
 */
/*
1.文件流管道，从文件中读取再写回文件，相当于中间做了ETL
2.读取文件仅支持OldCsv，且输入到文件仅支持追加流的方式。

 */
public class FlinkSQL06_Sink_File {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.定义文件连接器，读取文件
        tableEnv.connect(new FileSystem().path("sensor/sensor.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("fileInput");

        // 3.创建表
        Table table = tableEnv.from("fileInput");

        // 4.Table API
        Table tableResult = table.filter("id = 'sensor_1'").select("id,temp");

        // 5.SQL
        Table sqlResult = tableEnv.sqlQuery("select id,ts,temp from fileInput where id = 'sensor_1'");

        // 6.定义文件连接器，写入文件
        tableEnv.connect(new FileSystem().path("sensor/output.txt"))
                .withFormat(new OldCsv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts",DataTypes.BIGINT())
                        .field("temp",DataTypes.DOUBLE()))
                .createTemporaryTable("fileOutput");

        // 7.输出到文件系统
        sqlResult.insertInto("fileOutput");

        // 8.执行任务
        env.execute();
    }
}
