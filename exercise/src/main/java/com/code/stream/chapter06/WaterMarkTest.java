package com.code.stream.chapter06;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

public class WaterMarkTest {
    public static void main(String[] args) {
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 设置水位线生成时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        DataStreamSource<Event> source = env.fromElements(new Event("Bob", "/get", 1000L),
                new Event("alice", "/get", 2000L),
                new Event("tom", "/get/get", 3000L),
                new Event("tom", "/get/ounvc", 3500L),
                new Event("111", "/get/error", 10000L),
                new Event("222", "/getinfo", 7000L));
//        //  有序流的 waterMark 生成：使用 flink 自定义的
//        source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//            // 提取数据中的时间戳字段
//            @Override
//            public long extractTimestamp(Event element, long recordTimestamp) {
//                return element.getTimestamp();
//            }
//        }));
        // 乱序流的 waterMark 生成:使用 flink 自定义的
        source.assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event event, long l) {
                        return event.getTimestamp();
                    }
                }));

//        // 乱序流的 waterMark 生成
//        env.addSource(new ClickSource())
//                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
//                    @Override
//                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                        return null;
//                    }
//
//                    @Override
//                    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
//                        return WatermarkStrategy.super.createTimestampAssigner(context);
//                    }
//                });

//        // 乱序流的 waterMark 生成
//        env.addSource(new ClickSource())
//                .assignTimestampsAndWatermarks(new WatermarkStrategy<Event>() {
//                    @Override
//                    public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                        return null;
//                    }
//
//                    @Override
//                    public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
//                        return WatermarkStrategy.super.createTimestampAssigner(context);
//                    }
//                });
//        streamSource2.rebalance().print().setParallelism(4);
    }
}
