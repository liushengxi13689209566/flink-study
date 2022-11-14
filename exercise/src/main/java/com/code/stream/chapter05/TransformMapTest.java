package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class TransformMapTest {
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
                new Event("111", "/get/error", 10000L),
                new Event("222", "/getinfo", 7000L));

        SingleOutputStreamOperator<String> res = streamSource.map(new MyMapFunction());

        SingleOutputStreamOperator<String> res2 = streamSource.map((MapFunction<Event, String>) event -> event.user);

        res.print();

        env.execute();
    }

    public static class MyMapFunction implements MapFunction<Event, String> {
        @Override
        public String map(Event event) throws Exception {
            return event.user;
        }
    }
}
