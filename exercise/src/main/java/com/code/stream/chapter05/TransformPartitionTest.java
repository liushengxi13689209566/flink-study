package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransformPartitionTest {
    public static void main(String[] args) throws Exception {
        // 获取Flink批处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 6。自定义分区：
        env.fromElements(1, 2, 3, 4, 5, 6).partitionCustom(new Partitioner<Integer>() {
            @Override
            public int partition(Integer integer, int i) {
                return integer % 2;
            }
        }, new KeySelector<Integer, Integer>() {
            @Override
            public Integer getKey(Integer integer) throws Exception {
                return integer;
            }
        }).print().setParallelism(4);

        // 3.直接定义数据
//        DataStreamSource<Event> streamSource = env.fromElements(
//                new Event("Bob", "/get", 1000L),
//                new Event("alice", "/get", 2000L),
//                new Event("tom", "/get/get", 3000L),
//                new Event("tom", "/get/ounvc", 3500L),
//                new Event("tom", "/get/error", 10000L),
//                new Event("alice", "/get", 2000L),
//                new Event("Bob", "/get/error", 10000L),
//                new Event("alice", "/get", 2000L),
//                new Event("Bob", "/get/error", 100800L),
//                new Event("alice", "/get", 2000L),
//                new Event("tom", "/getinfo", 7000L));
//

//        // 1。shuffle 分区：随机分配
//        streamSource.shuffle().print().setParallelism(4);
//
//        // 2。轮询 分区
//        streamSource.rebalance().print().setParallelism(4);
//
//        //3。重缩放 分区：分组轮询
//        streamSource.rescale().print().setParallelism(4);
//
//        //4。广播：所有的分区都会收到
//        streamSource.broadcast().print().setParallelism(4);
//
//        // 5。全局分区：全部发送到某一个分区
//        streamSource.global().print().setParallelism(4);



        env.execute();
    }
}
