package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformRichfunctionTest {
    public static void main(String[] args) throws Exception {
        // 获取Flink批处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 3.直接定义数据
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Bob", "/get", 1000L),
                new Event("alice", "/get", 2000L),
                new Event("tom", "/get/get", 3000L),
                new Event("tom", "/get/ounvc", 3500L),
                new Event("tom", "/get/error", 10000L),
                new Event("alice", "/get", 2000L),
                new Event("Bob", "/get/error", 10000L),
                new Event("alice", "/get", 2000L),
                new Event("Bob", "/get/error", 100800L),
                new Event("alice", "/get", 2000L),
                new Event("tom", "/getinfo", 7000L));


        streamSource.map(new MyRichMapper()).setParallelism(2).print("");

        env.execute();
    }

    private static class MyRichMapper extends RichMapFunction<Event,Integer> {

        @Override
        public Integer map(Event event) throws Exception {
            return event.getUrl().length();
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            System.out.println("open" + getRuntimeContext().getIndexOfThisSubtask() +"号任务启动");
        }

        @Override
        public void close() throws Exception {
            super.close();
            System.out.println("close" + getRuntimeContext().getIndexOfThisSubtask() +"号任务启结束");
        }
    }
}
