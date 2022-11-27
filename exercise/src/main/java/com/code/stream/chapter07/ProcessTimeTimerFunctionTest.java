package com.code.stream.chapter07;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;

/**
 * @author oker
 */
public class ProcessTimeTimerFunctionTest {
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
        streamSource.keyBy(item -> item.getUser()).process(new KeyedProcessFunction<String, Event, String>() {
            @Override
            public void processElement(Event event, KeyedProcessFunction<String, Event, String>.Context context, Collector<String> collector) throws Exception {
                Long currentTime = context.timerService().currentProcessingTime();
                collector.collect(context.getCurrentKey() + "数据到达，到达时间是：" + new Timestamp(currentTime));
                // 注册一个10秒后的定时器
                context.timerService().registerProcessingTimeTimer(currentTime + 10 * 1000L);
            }

            @Override
            public void onTimer(long timestamp, KeyedProcessFunction<String, Event, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                out.collect(ctx.getCurrentKey() + "定时器触发，触发时间是：" + new Timestamp(timestamp));
            }

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
            }
        }).print("res");

        env.execute();
    }
}
