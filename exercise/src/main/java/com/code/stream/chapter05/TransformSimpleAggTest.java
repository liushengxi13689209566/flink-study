package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class TransformSimpleAggTest {
    public static void main(String[] args) throws Exception {
        // 获取Flink批处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 3.直接定义数据
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Bob", "/get", 1000L),
                new Event("alice", "/get", 2000L),
                new Event("tom", "/get/get", 3000L),
                new Event("tom", "/get/ounvc", 3500L),
                new Event("tom", "/get/error", 10000L),
                new Event("tom", "/getinfo", 7000L));

        streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) {
                return event.user;
            }
        }).max("timestamp").print("max: "); // max 只针对于处理时定义的字段进行修改

        streamSource.keyBy(new KeySelector<Event, String>() {
            @Override
            public String getKey(Event event) {
                return event.user;
            }
        }).maxBy("timestamp").print("maxBy: "); // maxBy 是针对于整个完整数据输出


        env.execute();
    }
}
