package com.example.datastream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

public class CustomSinkToMysql {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        DataStreamSource<String> data = executionEnvironment.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Student> out = data.map(new MapFunction<String, Student>() {
            @Override
            public Student map(String value) throws Exception {
                Student s = new Student();
                String[] split = value.split(",");

                s.setId(Integer.valueOf(split[0]));
                s.setName(split[1]);
                s.setAge(Integer.valueOf(split[2]));
                return s;
            }
        });

        DataStreamSink<Student> studentDataStreamSink = out.addSink(new SinkToMysql());

        executionEnvironment.execute("abcd");

    }


}