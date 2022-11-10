package com.code.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class HelloWorld {
    public static void main(String[] args) throws Exception {
        // 获取Flink批处理执行环境
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();
        // 获取文件中内容
        DataSource<String> dataSource = executionEnvironment.fromElements("hello", "world");
        // 对数据进行处理
        DataSet<Tuple2<String, Integer>> dataSet =
                dataSource.flatMap(new WordCountBatch.LineSplitter()).groupBy(0).sum(1);

        dataSet.writeAsCsv("file:///Users/tattoo/flink-study/exercise/src/main/data/output1.txt",
                "\n", " ").setParallelism(1);

        // 触发执行程序
        executionEnvironment.execute("wordcount batch process");
    }

    static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>>
                collector) throws Exception {
            for (String word : line.split(" ")) {
                collector.collect(new Tuple2<String, Integer>(word, 1));
            }
        }
    }
}
