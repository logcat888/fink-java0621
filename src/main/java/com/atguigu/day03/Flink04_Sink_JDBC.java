package com.atguigu.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.ArrayList;

/**
 * @author chenhuiup
 * @create 2020-11-18 17:48
 */
/*
自定义sink，实现将数据写入MySQL中
 */
public class Flink04_Sink_JDBC {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文本中读取数据
        DataStreamSource<String> input = env.readTextFile("sensor");

        //3.将数据写出到Mysql
        input.addSink(new MySinkToMysql());

        //4.执行
        env.execute();
    }

    public static class MySinkToMysql extends RichSinkFunction<String> {
        //声明MySQL相关的属性信息
        Connection connection;
        PreparedStatement updateInsert;

        @Override
        public void open(Configuration parameters) throws Exception {
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/test0621","root","123456");
            updateInsert = connection.prepareStatement("INSERT INTO flink(id,temp) " +
                    "values (?,?) ON duplicate key UPDATE temp=?");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            //分割数据
            String[] split = value.split(",");
            //给预编译SQL赋值
            updateInsert.setString(1,split[0]);
            updateInsert.setString(2,split[2]);
            updateInsert.setString(3,split[2]);
            //执行
            updateInsert.execute();
        }

        @Override
        public void close() throws Exception {
            updateInsert.close();
            connection.close();
        }
    }
}
