package com.example.dataset;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.List;

public class CacheApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String s = "file:///Users/baidu/Desktop/111.txt";
        environment.registerCachedFile(s, "cache");

        environment.fromElements("a", "b", "c", "d")
                .map(new RichMapFunction<String, String>() {
                    String s = "";

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);

                        File cache = getRuntimeContext().getDistributedCache().getFile("cache");

                        List<String> strings = FileUtils.readLines(cache);
                        s = strings.get(0);
                        strings.forEach(System.out::println);
                    }

                    @Override
                    public String map(String value) throws Exception {
                        return value + "=====" + s;
                    }
                })
                .print();

        System.out.println("---------");
    }
}