//package com.code.stream.window;
//
//import com.code.stream.entity.Entity;
//import org.apache.flink.api.java.functions.KeySelector;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
//import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//
//import java.util.Objects;
//
//public class TumblingWindow {
//    public static void main(String[] args) throws Exception {
//        // 获取Flink批处理执行环境
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//        // 获取输入内容
////        DataStreamSource<String> dataSource = executionEnvironment.fromElements("hello", "world");
//        DataStreamSource<Entity> dataSource = executionEnvironment.fromElements(
//                new Entity(1, "aaa"),
//                new Entity(2, "bbb"));
//        /*
//           1.滚动窗口设置
//        */
//        dataSource.keyBy(new KeySelector<Entity, Object>() {
//            @Override
//            public Object getKey(Entity value) throws Exception {
//                return value.id;
//            }
//        })
//                // 第一种方式
////                .timeWindow(Time.seconds(10))
//                // 第二种方式
//                .window(TumblingEventTimeWindows.of(Time.seconds(10)));
////                .process((it) -> {
////                });
//
//        /*
//           2.滑动窗口设置
//        */
//        dataSource.keyBy(_.id)
//                // 第一种方式
//                .timeWindow(Time.hours(1), Time.seconds(10))
//                // 第二种方式
//                .window(SlidingEventTimeWindows.of(Time.hours(1), Time.seconds(10)))
//                .process((it) -> {
//                });
//       /*
//           3.会话窗口设置
//        */
//        dataSource.keyBy(_.id)
//                // 第一种方式
//                .timeWindow(Time.hours(1), Time.seconds(10))
//                // 第二种方式
//                .window(EventTimeSessionWindows.withGap(Time.seconds(10)))
//                .process((it) -> {
//                });
//       /*
//           4.全局窗口设置：依赖于触发器进行触发
//        */
//        // 触发执行程序
//        executionEnvironment.execute("wordcount batch process");
//    }
//}
