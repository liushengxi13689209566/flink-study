package com.code.stream.chapter06;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;


public class WindowAggregateTest {
    public static void main(String[] args) throws Exception {
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置水位线生成时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> streamSource = env.addSource(new ClickSource())
                //  乱序流的 waterMark 生成：0秒延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
        // 最终目的：在10秒钟内产生的所有的数据，根据 user 求平均数
        streamSource.keyBy(data -> data.user)
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                // Tuple3<String, Long, Integer>  user：sum：个数
                // Tuple2<String, String> user：平均数
                .aggregate(new AggregateFunction<Event, Tuple3<String, Long, Integer>, Tuple2<String, String>>() {
                    @Override
                    public Tuple3<String, Long, Integer> createAccumulator() {
                        return Tuple3.of("", 0L, 0);
                    }

                    @Override
                    public Tuple3<String, Long, Integer> add(Event event, Tuple3<String, Long, Integer> value) {
                        // 返回改变之后的状态
                        return Tuple3.of(event.getUser(), value.f1 + event.getTimestamp(), value.f2 + 1);
                    }

                    @Override
                    public Tuple2<String, String> getResult(Tuple3<String, Long, Integer> value) {
                        Timestamp timestamp = new Timestamp(value.f1 / value.f2);
                        return Tuple2.of(value.f0, timestamp.toString());
                    }

                    @Override
                    public Tuple3<String, Long, Integer> merge(Tuple3<String, Long, Integer> stringLongIntegerTuple3, Tuple3<String, Long, Integer> acc1) {
                        return null;
                    }
                }).print();

        env.execute();
    }
}
