package com.example.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.core.fs.FileSystem;

public class DataSetFink {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String s = "file:///Users/baidu/Desktop/222";

        environment.generateSequence(0, 10)
                .writeAsText(s, FileSystem.WriteMode.OVERWRITE)
                .setParallelism(1)
        ;

        environment.execute("a1");

    }
}