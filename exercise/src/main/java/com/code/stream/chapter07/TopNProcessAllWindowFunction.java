package com.code.stream.chapter07;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.chapter06.UrlCountViewExampleTest;
import com.code.stream.entity.Event;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.collect.Maps;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.TreeMap;

/**
 * @author oker
 */
public class TopNProcessAllWindowFunction {
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
        streamSource.print("input:");
        /**
         * flink中window和windowall的区别
         在keyby后数据分流，window是把不同的key分开聚合成窗口，而windowall则把所有的key都聚合起来所以windowall的并行度只能为1，而window可以有多个并行度。*/
        streamSource.map(data -> data.getUser()).windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5))).aggregate(new MyAggregateFunction02(), new UvCountResult02()).print("");
        env.execute();
    }

    private static class MyAggregateFunction02 implements AggregateFunction<String, HashMap<String, Long>, HashMap<String, Long>> {
        @Override
        public HashMap<String, Long> createAccumulator() {
            return new HashMap<String, Long>();
        }

        @Override
        public HashMap<String, Long> add(String url, HashMap<String, Long> stringLongHashMap) {
            if (stringLongHashMap.containsKey(url)) {
                Long count = stringLongHashMap.get(url);
                stringLongHashMap.put(url, count + 1);
            } else {
                stringLongHashMap.put(url, 1L);
            }
            return stringLongHashMap;
        }

        @Override
        public HashMap<String, Long> getResult(HashMap<String, Long> stringLongHashMap) {
            return stringLongHashMap;
        }

        @Override
        public HashMap<String, Long> merge(HashMap<String, Long> stringLongHashMap, HashMap<String, Long> acc1) {
            return null;
        }
    }

    private static class UvCountResult02 extends ProcessAllWindowFunction<HashMap<String, Long>, String, TimeWindow> {
        @Override
        public void process(ProcessAllWindowFunction<HashMap<String, Long>, String, TimeWindow>.Context context, Iterable<HashMap<String, Long>> iterable, Collector<String> collector) throws Exception {

            // 排序并拿 hashMap 中的 前 N 个元素
            HashMap<String, Long> map = iterable.iterator().next();
            ArrayList<Tuple2<String, Long>> tuple2ArrayList = Lists.newArrayList();
            map.forEach((key, value) -> {
                tuple2ArrayList.add(new Tuple2<>(key, value));
            });
            tuple2ArrayList.sort(new Comparator<Tuple2<String, Long>>() {
                @Override
                public int compare(Tuple2<String, Long> o1, Tuple2<String, Long> o2) {
                    return o2.f1.intValue() - o1.f1.intValue();
                }
            });
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            System.out.println(("窗口开始时间：" + new Timestamp(start) + "结束时间：" + new Timestamp(end)));

            StringBuilder sb = new StringBuilder();
            sb.append("---------------------");
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Long> longTuple2 = tuple2ArrayList.get(i);
                String info = "NO: " + (i + 1) + " url:" + longTuple2.f0 + " count:" + longTuple2.f1 + "\n";
                sb.append(info);
            }
            collector.collect(sb.toString());
        }

    }
}
