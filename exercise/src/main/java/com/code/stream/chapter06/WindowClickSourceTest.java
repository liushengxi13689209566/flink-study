package com.code.stream.chapter06;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;


public class WindowClickSourceTest {
    public static void main(String[] args) throws Exception {
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置水位线生成时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> streamSource = env.addSource(new ClickSource())
//                env.fromElements(
//                          // event 单位是毫秒
//                        new Event("Bob", "/get", 1000L),
//                        new Event("alice", "/get", 2000L),
//                        new Event("tom", "/get/get", 3000L),
//                        new Event("tom", "/get/oun vc", 3500L),
//                        new Event("111", "/get/error", 10000L),
//                        new Event("Mary", "/get/error", 1000000L),
//                        new Event("222", "/getinfo", 7000L))
                //  乱序流的 waterMark 生成：2秒延迟，
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event element, long recordTimestamp) {
                                return element.getTimestamp();
                            }
                        }));
        streamSource.map(new MapFunction<Event, Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> map(Event value) throws Exception {
                        return Tuple2.of(value.getUser(), 1L);
                    }
                })
                .keyBy(data -> data.f0)
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // reduce 归约函数：从多 归约 到少
                .reduce(new ReduceFunction<Tuple2<String, Long>>() {
                    @Override
                    public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) {
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }).print();
        env.execute();
    }
}
