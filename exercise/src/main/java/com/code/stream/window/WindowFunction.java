package com.code.stream.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class WindowFunction {
    public static void main(String[] args) {
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取输入内容
        DataStreamSource<Tuple2<Integer, Long>> dataSource =
                env.fromElements(new Tuple2[]{new Tuple2<>(1, 2), new Tuple2<>(1, 2)});
        dataSource.keyBy(0)
                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(10)))
                .reduce(((value1, value2) -> {
                    return value1.f0, value1.f1 + value2.f1;
                }));


    }
}
