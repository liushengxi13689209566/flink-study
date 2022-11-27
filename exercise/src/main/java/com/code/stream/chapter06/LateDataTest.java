package com.code.stream.chapter06;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import com.code.stream.entity.UrlViewCount;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 *
 */
public class LateDataTest {
    public static void main(String[] args) throws Exception {
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置水位线生成时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> streamSource =
                env.socketTextStream("127.0.0.1",7709)
                        .map(new MapFunction<String, Event>() {
                            public Event map(String s) throws Exception {
                                String[] values = s.split(",");
                                return new Event(values[0].trim(), values[1].trim(),Long.valueOf(values[2].trim()));
                            }
                        })
//        SingleOutputStreamOperator<Event> streamSource = env.fromElements(
                        // event 单位是毫秒
//                        new Event("Bob", "/get", 1000L),
//                        new Event("alice", "/get", 2000L),
//                        new Event("tom", "/get/get", 3000L),
//                        new Event("tom", "/get/oun vc", 3500L),
//                        new Event("111", "/get/id=111", 10000L),
//                        new Event("333", "/get/id=111", 10000L),
//                        new Event("444", "/get/id=111", 10000L),
//                        new Event("Mary", "/get/error", 1000000L),
//                        new Event("222", "/getinfo", 7000L))
                //  乱序流的 waterMark 生成：2 秒延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                        .withTimestampAssigner(
                        new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));

        streamSource.print("data:");

        // 使用内部匿名类
        OutputTag<Event> outputTag = new OutputTag<Event>("late"){};

        SingleOutputStreamOperator<UrlViewCount> result = streamSource.keyBy(Event::getUrl)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .allowedLateness(Time.minutes(1))
                .sideOutputLateData(outputTag)
                .aggregate(new MyAggregateFunction(), new UvCountResult());

        result.print("");
        result.getSideOutput(outputTag).print("late:");

        env.execute();
    }

    private static class MyAggregateFunction implements AggregateFunction<Event, Long, Long> {
        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(Event event, Long aLong) {
            aLong++;
            return aLong;
        }

        @Override
        public Long getResult(Long aLong) {
            return aLong;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    private static class UvCountResult extends ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow> {
        @Override
        public void process(String url, ProcessWindowFunction<Long, UrlViewCount, String, TimeWindow>.Context context, Iterable<Long> iterable, Collector<UrlViewCount> collector) throws Exception {
            Long count = iterable.iterator().next();
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            UrlViewCount viewCount = new UrlViewCount(url, count, start, end);
            collector.collect(viewCount);
        }
    }
}
/**
 * 输入数据：
 * ➜  ~ nc -lk 7709
 * mary, ./1, 1000
 * mary, ./1, 2000
 * Bob, ./2222 ,4000
 * Tom, ./333 ,5000
 * Tom, ./333 ,10000
 * Tom, ./333 ,12000
 * Tom, ./333 ,2000
 * mary, ./1, 1000
 * mary, ./1, 70000
 * mary, ./1, 7000
 * mary, ./1, 72000
 * mary, ./1, 7000
 *
 * 输出数据：
 * data:> Event{user='mary', url='./1', timestamp=1970-01-01 08:00:01.0}
 * data:> Event{user='mary', url='./1', timestamp=1970-01-01 08:00:02.0}
 * data:> Event{user='Bob', url='./2222', timestamp=1970-01-01 08:00:04.0}
 * data:> Event{user='Tom', url='./333', timestamp=1970-01-01 08:00:05.0}
 * data:> Event{user='Tom', url='./333', timestamp=1970-01-01 08:00:10.0}
 * data:> Event{user='Tom', url='./333', timestamp=1970-01-01 08:00:12.0} //达到 0～10s 的水位线
 * UrlViewCount{url='./1', count=2, winStart=1970-01-01 08:00:00.0, winEnd=1970-01-01 08:00:10.0}
 * UrlViewCount{url='./333', count=1, winStart=1970-01-01 08:00:00.0, winEnd=1970-01-01 08:00:10.0}
 * // 因为10s内的数据就只有一个，所以就是 1
 * UrlViewCount{url='./2222', count=1, winStart=1970-01-01 08:00:00.0, winEnd=1970-01-01 08:00:10.0}
 *  data:> Event{user='Tom', url='./333', timestamp=1970-01-01 08:00:02.0}
 * UrlViewCount{url='./333', count=2, winStart=1970-01-01 08:00:00.0, winEnd=1970-01-01 08:00:10.0}
 * // 0～10 秒的窗口在12秒的时候触发了计算，此数据属于迟到数据，因此直接发生计算。
 * data:> Event{user='mary', url='./1', timestamp=1970-01-01 08:00:01.0}
 * UrlViewCount{url='./1', count=3, winStart=1970-01-01 08:00:00.0, winEnd=1970-01-01 08:00:10.0}
 * data:> Event{user='mary', url='./1', timestamp=1970-01-01 08:01:10.0}
 * UrlViewCount{url='./333', count=2, winStart=1970-01-01 08:00:10.0, winEnd=1970-01-01 08:00:20.0}
 * data:> Event{user='mary', url='./1', timestamp=1970-01-01 08:00:07.0}
 * UrlViewCount{url='./1', count=4, winStart=1970-01-01 08:00:00.0, winEnd=1970-01-01 08:00:10.0}
 * data:> Event{user='mary', url='./1', timestamp=1970-01-01 08:01:12.0}
 * data:> Event{user='mary', url='./1', timestamp=1970-01-01 08:00:07.0}
 * late:> Event{user='mary', url='./1', timestamp=1970-01-01 08:00:07.0}
* */