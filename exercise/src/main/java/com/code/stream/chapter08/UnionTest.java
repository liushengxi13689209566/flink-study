package com.code.stream.chapter08;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author oker
 */
public class UnionTest {
    public static void main(String[] args) throws Exception {
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置水位线生成时间间隔
        env.getConfig().setAutoWatermarkInterval(100);

        SingleOutputStreamOperator<Event> streamSource1 = env.socketTextStream("127.0.0.1", 7709).map(new MapFunction<String, Event>() {
                    public Event map(String s) throws Exception {
                        String[] values = s.split(",");
                        return new Event(values[0].trim(), values[1].trim(), Long.valueOf(values[2].trim()));
                    }
                })
                //  乱序流的 waterMark 生成：2秒延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
        streamSource1.print("input-1:");


        SingleOutputStreamOperator<Event> streamSource2 = env.socketTextStream("127.0.0.1", 8888).map(new MapFunction<String, Event>() {
                    public Event map(String s) throws Exception {
                        String[] values = s.split(",");
                        return new Event(values[0].trim(), values[1].trim(), Long.valueOf(values[2].trim()));
                    }
                })
                //  乱序流的 waterMark 生成：5秒延迟
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ofSeconds(5)).withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                    @Override
                    public long extractTimestamp(Event element, long recordTimestamp) {
                        return element.getTimestamp();
                    }
                }));
        streamSource2.print("input-2:");

        // 合并两条流
        streamSource1.union(streamSource2).process(new ProcessFunction<Event, String>() {
            @Override
            public void processElement(Event event, ProcessFunction<Event, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect("当前水位线是：" + context.timerService().currentWatermark());
            }
        }).print();

        env.execute();

    }
}
