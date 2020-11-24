package com.atguigu.day06;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Elasticsearch;
import org.apache.flink.table.descriptors.Json;
import org.apache.flink.table.descriptors.Schema;

/**
 * @author chenhuiup
 * @create 2020-11-23 18:22
 */
/*
流式管道：从端口中读取数据，写入到ES，并指定的追加模式
bulkFlushMaxActions(1))//设置最大刷写个数
 */
public class FlinkSQL08_Sink_ES_Append {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 2.创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 3.从端口读取数据，并转换为JavaBean对象
        SingleOutputStreamOperator<SensorReading> socketDS = env.socketTextStream("hadoop102", 7777).map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
        });

        // 4.创建表
        tableEnv.createTemporaryView("sensor", socketDS);

        // 5.执行SQL
        Table table = tableEnv.sqlQuery("select id,ts,temp from sensor");

        // 6.定义Es的连接器，并指定为追加模式，设置1个数据写入一次Es
        tableEnv.connect(new Elasticsearch()
                .version("6")
                .host("hadoop102",9200,"http") //指定连接地址
                .index("sensor01")
                .documentType("_doc")
                .bulkFlushMaxActions(1))//设置最大刷写个数
                .withFormat(new Json())
                .inAppendMode()  //指定追加模式
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("ts", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE()))
                .createTemporaryTable("EsOutput");

        // 7.写入到Es
        table.insertInto("EsOutput");

        // 8.执行任务
        env.execute();
    }
}
