package com.code.stream.chapter09;

import com.code.stream.chapter05.ClickSource;
import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class StateTest {
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
        streamSource.keyBy(data -> data.getUser()).flatMap(new MyFlatMapFunction());

        env.execute();
    }

    private static class MyFlatMapFunction extends RichFlatMapFunction<Event, String> {
        // 定义状态
        ValueState<Event> myValue;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Event> stateDescriptor = new ValueStateDescriptor<>("my-state", Event.class);
            myValue = getRuntimeContext().getState(stateDescriptor);
            StateTtlConfig stateTtlConfig = StateTtlConfig.newBuilder(Time.hours(1)) //此处状态只能是处理时间
                    // 设置什么情况下更新 TTL
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                    // 可见性：返回什么样的数据
                    .setStateVisibility(StateTtlConfig.StateVisibility.ReturnExpiredIfNotCleanedUp)
                    .build();
            /** 为描述器设置 TTL */
            stateDescriptor.enableTimeToLive(stateTtlConfig);
        }

        @Override
        public void flatMap(Event event, Collector<String> collector) throws Exception {
            System.out.println("前的:" + myValue.value());
            myValue.update(event);
            System.out.println("更新后的:" + myValue.value());
            System.out.println("------------------------%%------------------------");
        }


    }

}
