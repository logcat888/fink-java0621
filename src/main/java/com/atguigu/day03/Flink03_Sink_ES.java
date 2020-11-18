package com.atguigu.day03;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author chenhuiup
 * @create 2020-11-18 14:33
 */
/*
将数据写入ES中
 */
public class Flink03_Sink_ES {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文本中读取数据
        DataStreamSource<String> input = env.readTextFile("sensor");

        //3.将数据写出到ES
        //3.1 ES连接参数
        ArrayList<HttpHost> httpHosts = new ArrayList<>();
        httpHosts.add(new HttpHost("hadoop102", 9200));

        //3.2 构建ElasticsearchSink
        ElasticsearchSink<String> build = new ElasticsearchSink
                .Builder<>(httpHosts, new MyElasticsearchSinkFunction()).build();

        //3.3 写入数据操作
        input.addSink(build);

        //4.执行
        env.execute();
    }

    public static class MyElasticsearchSinkFunction implements ElasticsearchSinkFunction<String> {

        @Override
        public void process(String element, RuntimeContext ctx, RequestIndexer indexer) {

            //对元素分割处理
            String[] split = element.split(",");
            //创建Map用于存放待存储到ES的数据
            HashMap<String, String> map = new HashMap<>();
            map.put("id", split[0]);
            map.put("ts", split[1]);
            map.put("temp", split[2]);

            //创建IndexRequest
            IndexRequest indexRequest = new IndexRequest().index("sensor")
                    .type("_doc")
                    .id("")
                    .source(map);
            //将数据写入
            indexer.add(indexRequest);

        }
    }
}
