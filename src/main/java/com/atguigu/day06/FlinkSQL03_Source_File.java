package com.atguigu.day06;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:21
 */
/*
定义文件连接器从文件中读取数据创建表
定义连接器步骤：
   tableEnv.connect().withFormat().withSchema().createTemporaryTable("表明")
 Table类型：1）从流转换；2）从表转换
 */
public class FlinkSQL03_Source_File {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.定义文件连接器
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
        Table sqlResult = tableEnv.sqlQuery("select id,temp from fileInput where id = 'sensor_1'");

        // 6.打印
        tableEnv.toAppendStream(tableResult, Row.class).print("tableResult");
        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");

        // 7.执行任务
        env.execute();

    }
}
