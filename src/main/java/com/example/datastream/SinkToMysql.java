package com.example.datastream;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

public class SinkToMysql extends RichSinkFunction<Student> {

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("----------------open");
    }


    @Override
    public void close() throws Exception {
        System.out.println("----------------close");
    }


    @Override
    public void invoke(Student value, Context context) throws Exception {
        System.out.println("----------------invoke");

        System.out.println(value);

    }

}