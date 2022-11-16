package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformReduceTest {
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
                new Event("alice", "/get", 2000L),
                new Event("Bob", "/get/error", 10000L),
                new Event("alice", "/get", 2000L),
                new Event("Bob", "/get/error", 100800L),
                new Event("alice", "/get", 2000L),
                new Event("tom", "/getinfo", 7000L));


        SingleOutputStreamOperator<Tuple2<String, Long>> clicksByUser = streamSource.map(new MapFunction<Event, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Event event) throws Exception {
                return Tuple2.of(event.getUser(), 1L);
            }
        }).keyBy(it -> it.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });
        clicksByUser.print("1111:");

        clicksByUser.keyBy(it -> "key").reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return value1.f1 > value2.f1 ? value1 : value2;
            }
        }).print("222");

        env.execute();
    }
}
