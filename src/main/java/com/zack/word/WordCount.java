package com.zack.word;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * com.zack
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/2 17:23
 */
public class WordCount {
    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        String filePath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\hello.txt";
        DataSource<String> wordCountDataSet = env.readTextFile(filePath);
        wordCountDataSet.getSplitDataProperties().splitsGroupedBy();

    }
}
