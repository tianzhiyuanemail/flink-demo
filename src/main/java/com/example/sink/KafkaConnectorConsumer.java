package com.example.sink;

import com.example.pojo.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaConnectorConsumer {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "test";

        SimpleStringSchema simpleStringSchema = new SimpleStringSchema();

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", "39.102.32.141:9092");
        properties.setProperty("group.id", "group-1");
        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(topic, simpleStringSchema, properties);
        kafkaConsumer.setStartFromEarliest();

        executionEnvironment
                .addSource(kafkaConsumer)
                .map(new MapFunction<String, Student>() {
                    @Override
                    public Student map(String value) throws Exception {
                        Student s = new Student();
                        String[] split = value.split(",");

                        s.setId(Integer.valueOf(split[0]));
                        s.setName(split[1]);
                        s.setAge(Integer.valueOf(split[2]));
                        return s;
                    }
                })
        .print();

        executionEnvironment.execute("qqqq");
    }
}