package com.code.stream.chapter05;

import com.code.stream.entity.Event;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

public class SourceTest {
    public static void main(String[] args) throws Exception {
        // 获取Flink批处理执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 1.获取文件中内容
        DataStreamSource<String> dataSource = env.readTextFile("src/main/data/clicks.txt");
        dataSource.print();
        // 2.自定义数据结构
        List<Integer> nums = Lists.newArrayList();
        nums.add(1);
        nums.add(2);
        DataStreamSource<Integer> integerDataStreamSource = env.fromCollection(nums);
        integerDataStreamSource.print();

        // 3.直接定义数据
        DataStreamSource<Event> streamSource = env.fromElements(
                new Event("Bob", "/get", 1000L),
                new Event("alice", "/get", 2000L),
                new Event("tom", "/get/get", 3000L),
                new Event("tom", "/get/ounvc", 3500L),
                new Event("111", "/get/error", 10000L),
                new Event("222", "/getinfo", 7000L));

        env.execute();

    }
}
