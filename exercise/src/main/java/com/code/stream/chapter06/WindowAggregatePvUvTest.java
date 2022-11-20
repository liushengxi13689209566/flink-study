package com.code.stream.chapter06;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.shaded.guava18.com.google.common.collect.Sets;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * 1、pv的全称是page view，译为页面浏览量或点击量，通常是衡量一个网站甚至一条网络新闻的指标。用户每次对网站中的一个页面的请求或访问均被记录1个PV，
 * 用户对同一页面的多次访问，pv累计。例如，用户访问了4个页面，pv就+4
 * <p>
 * 2、uv 的全称是unique view，译为通过互联网访问、浏览这个网页的自然人，访问网站的一台电脑客户端被视为一个访客，在同一天内相同的客户端只被计算一次。
 * <p>
 * 统计 PV 和 UV，求 PV/UV 的值，得到平均人次点击访问次数
 */
public class WindowAggregatePvUvTest {
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

        // 最终目的：在10秒钟内产生的所有的数据，根据 user 求平均数
        streamSource.keyBy(data -> true)
                // 滑动事件时间窗口
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                // Tuple2<Long, HashSet<String>> pv: uv (hashSet 表示)
                .aggregate(new AggregateFunction<Event, Tuple2<Long, HashSet<String>>, Double>() {
                    @Override
                    public Tuple2<Long, HashSet<String>> createAccumulator() {
                        return Tuple2.of(0L, Sets.newHashSet());
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> add(Event event, Tuple2<Long, HashSet<String>> value) {
                        value.f1.add(event.getUser());
                        return Tuple2.of(value.f0 + 1, value.f1);
                    }

                    @Override
                    public Double getResult(Tuple2<Long, HashSet<String>> value) {
                        return Double.valueOf(value.f0 / value.f1.size());
                    }

                    @Override
                    public Tuple2<Long, HashSet<String>> merge(Tuple2<Long, HashSet<String>> longHashSetTuple2, Tuple2<Long, HashSet<String>> acc1) {
                        return null;
                    }

                }).print();

        env.execute();
    }
}

/**
 * SLF4J: Failed to load class "org.slf4j.impl.StaticLoggerBinder".
 * SLF4J: Defaulting to no-operation (NOP) logger implementation
 * SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.
 *
 * data:> Event{user='Bob', url='./home', timestamp=2022-11-20 15:30:35.78}
 * data:> Event{user='Tom', url='./cart', timestamp=2022-11-20 15:30:36.789} // (1)滑动窗口关闭的是：【15:30:26-15:30:36）
 * 1.0
 * data:> Event{user='Tom', url='./home', timestamp=2022-11-20 15:30:37.797}
 * data:> Event{user='Tom', url='./prod?id=100', timestamp=2022-11-20 15:30:38.801} // (2)滑动窗口关闭的是：【15:30:28-15:30:38）
 * 1.0
 * data:> Event{user='Bob', url='./prod?id=100', timestamp=2022-11-20 15:30:39.805}
 * data:> Event{user='Alice', url='./cart', timestamp=2022-11-20 15:30:40.811} // (3)滑动窗口关闭的是：【15:30:30-15:30:40） 仔细观察：以上的所有数据都会在这个区间中
 * 2.0
 * data:> Event{user='Alice', url='./cart', timestamp=2022-11-20 15:30:41.822}
 * data:> Event{user='Bob', url='./fav', timestamp=2022-11-20 15:30:42.828} // (4)滑动窗口关闭的是：【15:30:32-15:30:42）
 * 2.0
 * data:> Event{user='Tom', url='./prod?id=100', timestamp=2022-11-20 15:30:43.835}
 * data:> Event{user='Alice', url='./prod?id=100', timestamp=2022-11-20 15:30:44.841} // (5)滑动窗口关闭的是：【15:30:34-15:30:44）仔细观察：以上的所有数据都会在这个区间中
 * 3.0
 * data:> Event{user='Alice', url='./fav', timestamp=2022-11-20 15:30:45.848}
 * data:> Event{user='Bob', url='./home', timestamp=2022-11-20 15:30:46.854}  // (6)滑动窗口关闭的是：【15:30:36-15:30:46）
 * 3.0
 * data:> Event{user='Tom', url='./prod?id=100', timestamp=2022-11-20 15:30:47.859}
 * data:> Event{user='Tom', url='./cart', timestamp=2022-11-20 15:30:48.866} // (7)滑动窗口关闭的是：【15:30:38-15:30:48）
 * 3.0
 * data:> Event{user='Bob', url='./prod?id=100', timestamp=2022-11-20 15:30:49.872}
 * data:> Event{user='Bob', url='./prod?id=100', timestamp=2022-11-20 15:30:50.878}
 * 3.0
 * data:> Event{user='Alice', url='./cart', timestamp=2022-11-20 15:30:51.883}
 * data:> Event{user='Alice', url='./cart', timestamp=2022-11-20 15:30:52.887}
 * 3.0
 * data:> Event{user='Tom', url='./cart', timestamp=2022-11-20 15:30:53.894}
 * data:> Event{user='Tom', url='./home', timestamp=2022-11-20 15:30:54.898}
 * 3.0
 * data:> Event{user='Tom', url='./cart', timestamp=2022-11-20 15:30:55.905}
 * data:> Event{user='Alice', url='./fav', timestamp=2022-11-20 15:30:56.911}
 * 3.0
 * data:> Event{user='Bob', url='./home', timestamp=2022-11-20 15:30:57.916}
 * data:> Event{user='Bob', url='./home', timestamp=2022-11-20 15:30:58.923}
 *
 * 进程已结束,退出代码137 (interrupted by signal 9: SIGKILL)
 */