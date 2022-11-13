package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SourceCustomTest {
    public static void main(String[] args) throws Exception {
        // 获取Flink批处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        //自定义实现 DataSource
//        DataStreamSource<Event> dataStreamSource = env.addSource(new ClickSource());
//        dataStreamSource.print();

        //自定义实现 并行 DataSource
        DataStreamSource<Event> dataStreamSource2 = env.addSource(new ClickParallelSource()).setParallelism(10);

        dataStreamSource2.print();

        env.execute();

    }
}
