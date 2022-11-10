//package com.code.stream.chapter06;
//
//import com.code.stream.entity.Event;
//import org.apache.flink.api.common.eventtime.*;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//public class WaterMarkTest {
//    public static void main(String[] args) {
//        // 获取 Flink 流处理执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 设置水位线生成时间间隔
//        env.getConfig().setAutoWatermarkInterval(100);
//
//        DataStreamSource<Event> streamSource = (DataStreamSource<Event>) env.fromElements(new Event("Bob", "/get", 1000L),
//                new Event("alice", "/get", 2000L),
//                new Event("tom", "/get/get", 3000L),
//                new Event("tom", "/get/ounvc", 3500L),
//                new Event("111", "/get/error", 10000L),
//                new Event("222", "/getinfo", 7000L))
//                //  有序流的 waterMark 生成
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forMonotonousTimestamps()
//                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
//                            @Override
//                            public long extractTimestamp(Event element, long recordTimestamp) {
//                                return element.getTimestamp();
//                            }
//                        }));
//
//        DataStreamSource<Event> streamSource2 = env.addSource(new ClickSource())
//                .assignTimestampsAndWatermarks(new WatermarkStrategy<Object>() {
//                    @Override
//                    public WatermarkGenerator<Object> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
//                        return null;
//                    }
//
//                    @Override
//                    public TimestampAssigner<Object> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
//                        return WatermarkStrategy.super.createTimestampAssigner(context);
//                    }
//                });
//        streamSource2.rebalance().print().setParallelism(4);
//    }
//}
