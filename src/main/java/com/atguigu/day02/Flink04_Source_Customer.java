package com.atguigu.day02;

import com.atguigu.bean.SensorReading;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author chenhuiup
 * @create 2020-11-17 11:54
 */
/*
自定义source源
 */
public class Flink04_Source_Customer {
    public static void main(String[] args) throws Exception {
        // 1. 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.从自定义source源中读取数据
        DataStreamSource<SensorReading> sensorDS = env.addSource(new ComstuomSource());

        // 3.打印
        sensorDS.print();

        // 4.执行任务
        env.execute("Flink04_Source_Customer");
    }

    //自定义source源
    public static class ComstuomSource implements SourceFunction<SensorReading> {

        // 定义标记位
        private boolean running = true;
        private Random random = new Random();

        @Override
        public void run(SourceContext<SensorReading> ctx) throws Exception {
            // 造10个传感器,放入HashMap中
            HashMap<String, Double> map = new HashMap<>();
            for (int i = 0; i < 10; i++) {
                map.put("sensor" + i, 50 + random.nextGaussian() * 20);
            }

            while (running) {
                for (String s : map.keySet()) {
                    double temp = map.get(s) + random.nextGaussian();
                    ctx.collect(new SensorReading(s, System.currentTimeMillis(), temp));
                }
                //睡一会
                TimeUnit.SECONDS.sleep(2);
            }

        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}

