package com.code.stream.chapter06;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;

/**
 * 统计 PV 和 UV
 */
public class WindowProcessTest {
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
        streamSource.print("data:");

        streamSource.keyBy(data -> true).window(TumblingEventTimeWindows.of(Time.seconds(10))).process(new MyProcessWindowFunction()).print("");


        env.execute();
    }

    private static class MyProcessWindowFunction extends ProcessWindowFunction<Event, String, Boolean, TimeWindow> {
        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<Event, String, Boolean, TimeWindow>.Context context, Iterable<Event> iterable, Collector<String> collector) throws Exception {
            Set<String> set = Sets.newHashSet();
            for (Event event : iterable) {
                set.add(event.getUser());
            }
            Integer uv = set.size();
            Long start = context.window().getStart();
            Long end = context.window().getEnd();

            collector.collect("窗口开始时间：" + new Timestamp(start) + "结束时间：" + new Timestamp(end) + "uv:" + uv);
        }
    }
}
/**
 * data:> Event{user='Alice', url='./prod?id=100', timestamp=2022-11-21 22:23:58.189}
 * data:> Event{user='Mary', url='./cart', timestamp=2022-11-21 22:23:59.197}
 * data:> Event{user='Mary', url='./cart', timestamp=2022-11-21 22:24:00.202}
 * 窗口开始时间：2022-11-21 22:23:50.0结束时间：2022-11-21 22:24:00.0uv:2
 * data:> Event{user='Tom', url='./prod?id=100', timestamp=2022-11-21 22:24:01.208}
 * data:> Event{user='Bob', url='./fav', timestamp=2022-11-21 22:24:02.209}
 * data:> Event{user='Tom', url='./cart', timestamp=2022-11-21 22:24:03.216}
 * data:> Event{user='Tom', url='./fav', timestamp=2022-11-21 22:24:04.224}
 * data:> Event{user='Tom', url='./prod?id=100', timestamp=2022-11-21 22:24:05.228}
 * data:> Event{user='Mary', url='./prod?id=100', timestamp=2022-11-21 22:24:06.236}
 * data:> Event{user='Bob', url='./fav', timestamp=2022-11-21 22:24:07.242}
 * data:> Event{user='Mary', url='./fav', timestamp=2022-11-21 22:24:08.249}
 * data:> Event{user='Bob', url='./home', timestamp=2022-11-21 22:24:09.255}
 * data:> Event{user='Alice', url='./home', timestamp=2022-11-21 22:24:10.259} // 注意这里的 Alice 是没有被包含进来的，所以这个时间戳口的 uv 是 3
 * 窗口开始时间：2022-11-21 22:24:00.0结束时间：2022-11-21 22:24:10.0uv:3
 * data:> Event{user='Tom', url='./home', timestamp=2022-11-21 22:24:11.265}
 * data:> Event{user='Bob', url='./home', timestamp=2022-11-21 22:24:12.27}
 * data:> Event{user='Bob', url='./prod?id=100', timestamp=2022-11-21 22:24:13.273}
 * data:> Event{user='Mary', url='./cart', timestamp=2022-11-21 22:24:14.279}
 * data:> Event{user='Bob', url='./fav', timestamp=2022-11-21 22:24:15.285}
 * data:> Event{user='Alice', url='./home', timestamp=2022-11-21 22:24:16.288}
 * data:> Event{user='Alice', url='./home', timestamp=2022-11-21 22:24:17.295}
 * data:> Event{user='Alice', url='./prod?id=100', timestamp=2022-11-21 22:24:18.3}
 */