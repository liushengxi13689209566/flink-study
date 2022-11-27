package com.code.stream.chapter08;

import com.code.stream.entity.Event;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @author oker
 */
public class ConnectTest {
    public static void main(String[] args) throws Exception {
        // 获取 Flink 流处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 设置水位线生成时间间隔
        env.getConfig().setAutoWatermarkInterval(100);


        SingleOutputStreamOperator<Integer> streamSource1 = env.fromElements(1, 2, 3);
        SingleOutputStreamOperator<Long> streamSource2 = env.fromElements(4L, 5L, 6L, 7L);

        streamSource1.connect(streamSource2).process(new CoProcessFunction<Integer, Long, String>() {
            @Override
            public void processElement1(Integer integer, CoProcessFunction<Integer, Long, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect("Integer:" + integer);
            }

            @Override
            public void processElement2(Long aLong, CoProcessFunction<Integer, Long, String>.Context context, Collector<String> collector) throws Exception {
                collector.collect("Long:" + aLong);
            }
        }).print();

        env.execute();

    }
}
