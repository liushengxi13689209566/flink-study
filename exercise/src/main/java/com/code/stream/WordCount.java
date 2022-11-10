//package com.code.stream;
//
//import org.apache.flink.api.java.DataSet;
//import org.apache.flink.api.java.ExecutionEnvironment;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.scala.DataStream;
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment;
//
//import java.util.Objects;
//
//public class WordCount {
//    public static void main(String[] args) throws Exception {
//        // 第一个参数为输入路径，第二个参数为输出路径
//        String inPath = "src/main/data/input.txt";
//        String outPath = "src/main/data/output1.txt";
//        // 获取Flink批处理执行环境
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        // 获取文件中内容
//        DataStream<String> text = env.readTextFile("src/main/data/input.txt");
//        text.print();
//
////        text.flatMap().filter(Objects::nonNull).map((_,1))
//
//        // 对数据进行处理
////        DataSet<Tuple2<String, Integer>> dataSet = text.flatMap(new WordCountBatch.LineSplitter()).groupBy(0).sum(1);
////        dataSet.writeAsCsv(outPath, "\n", " ").setParallelism(1);
//        // 触发执行程序
//        env.execute("wordcount batch process");
//    }
//}
