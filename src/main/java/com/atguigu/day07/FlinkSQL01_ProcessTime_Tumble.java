package com.atguigu.day07;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author chenhuiup
 * @create 2020-11-24 15:42
 */
/*
分组窗口（group Window）：
1.时间窗口：
    1）滚动窗口
    2）滑动窗口
    3）会话窗口
2.计数窗口：
    1）滚动窗口
    2）滑动窗口
3. 创建Table或表时需要指定时间字段，是事件时间（rowtime）还是处理时间（proctime）
    Table table = tableEnv.fromDataStream(sensorDS,"id,ts,temp,pt.proctime");

4. table API
1)tw.end,tw.start是窗口的开始时间和结束时间
2）groupBy后必须加上窗口别名，可以不加表的字段
3) table.window(Tumble.over("5.seconds").on("pt").as("tw")) seconds写单数或者复数都行
5.SQL
1)Tumble_end(pt,interval '5' second) 是窗口的结束时间
2）group by id,Tumble(pt,interval '5' second) 在group by 定义窗口大小，时间单位是单数,pt为定义的时间字段
 */
public class FlinkSQL01_ProcessTime_Tumble {
    public static void main(String[] args) throws Exception {
        // 1.创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2.从端口获取数据,转换为JavaBean
        SingleOutputStreamOperator<SensorReading> sensorDS = env.socketTextStream("hadoop102", 7777)
                .map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]));
                });

        // 3.将流转换为表,追加处理时间
        Table table = tableEnv.fromDataStream(sensorDS,"id,ts,temp,pt.proctime");

        // 4.Table API
        // a.按照时间滚动
        Table result = table.window(Tumble.over("5.seconds").on("pt").as("tw"))
                .groupBy("tw,id")
                .select("id,id.count,tw.end,tw.start");
        // b.按照个数滚动
//        Table resultCount = table.window(Tumble.over("5.rows").on("pt").as("tw"))
//                .groupBy("id,tw")
//                .select("id,id.count");

        // 5.SQL
        // 0.注册表
        tableEnv.createTemporaryView("sensor",table);
        // a.按照时间滚动
//        Table sqlResult = tableEnv.sqlQuery("select id,count(id) as ct ,Tumble_end(pt,interval '5' second) " +
//                "from sensor group by id,Tumble(pt,interval '5' second)");

        // b.按照个数滚动(好像没有)

        // 5.转换为流进行输出
        tableEnv.toAppendStream(result, Row.class).print("result");
//        tableEnv.toAppendStream(resultCount, Row.class).print("resultCount");
//        tableEnv.toAppendStream(sqlResult, Row.class).print("sqlResult");


        // 6.执行
        env.execute();

    }
}
