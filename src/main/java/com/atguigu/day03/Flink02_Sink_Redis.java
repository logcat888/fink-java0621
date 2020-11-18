package com.atguigu.day03;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisConfigBase;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author chenhuiup
 * @create 2020-11-18 14:32
 */
/*
将数据写入redis中
 */
public class Flink02_Sink_Redis {
    public static void main(String[] args) throws Exception {
        //1.获取执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.从文本中读取数据
        DataStreamSource<String> input = env.readTextFile("sensor");

        //3.将数据写出到redis
        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("hadoop102")
                .setPort(7777).build();
        DataStreamSink<String> redis = input.addSink(new RedisSink<String>(config, new MyRedisMapper()));

        //4.执行
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<String>{

        @Override
        public RedisCommandDescription getCommandDescription() {
            // 选用redis中map结构存数据，参数分别为Map，外部key
            return new RedisCommandDescription(RedisCommand.HSET,"sensor");
        }

        @Override
        public String getKeyFromData(String data) {
            return data.split(",")[0];
        }

        @Override
        public String getValueFromData(String data) {
            return data.split(",")[2];
        }
    }
}
