package com.code.stream.helloworld;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class UnBoundedStreamWordCount {
    public static void main(String[] args) throws Exception {
        //1. 获取Flink批处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 获取文件中内容
        DataStreamSource<String> dataSource = env.readTextFile("src/main/data/input.txt");

        // 对数据进行处理
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOneTuple = dataSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> out) {
                String[] words = line.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1L));
                }
            }
        }).returns(Types.TUPLE(Types.STRING, Types.LONG));

        //  按照 word 分组
        KeyedStream<Tuple2<String, Long>, Object> wordAndOneGroup = wordAndOneTuple.keyBy(data -> data.f0);

        //分组内聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> res = wordAndOneGroup.sum(1);

        res.print();

        env.execute();

    }
}
