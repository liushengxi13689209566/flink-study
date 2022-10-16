package com.code.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @Author CaiWencheng
 * @Date 2022-05-04 22:52
 */
public class WordCountBatch {
    public static void main(String[] args) throws Exception {
        // 第一个参数为输入路径，第二个参数为输出路径
        String inPath = "/flink-study/exercise/src/main/data/hello.txt";
        String outPath = "/flink-study/exercise/src/main/data/output.txt";
        // 获取Flink批处理执行环境
        ExecutionEnvironment executionEnvironment =
                ExecutionEnvironment.getExecutionEnvironment();
        // 获取文件中内容
        DataSet<String> text = executionEnvironment.readTextFile(inPath);
        // 对数据进行处理
        DataSet<Tuple2<String, Integer>> dataSet = text.flatMap(new LineSplitter()).groupBy(0).sum(1);
        dataSet.writeAsCsv(outPath,"\n"," ").setParallelism(1);
        // 触发执行程序
        executionEnvironment.execute("wordcount batch process");
    }
    static class LineSplitter implements FlatMapFunction<String, Tuple2<String,Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>>
                collector) throws Exception {
            for (String word:line.split(" ")) {
                collector.collect(new Tuple2<String, Integer>(word,1));
            }
        }
    }
}

