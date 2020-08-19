package com.example.sink;

import com.example.pojo.Student;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class KafkaConnectorProvider {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        String topic = "p1";

        SimpleStringSchema simpleStringSchema = new SimpleStringSchema();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "39.102.32.141:9092");

        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(topic, simpleStringSchema, properties);

        kafkaProducer.setWriteTimestampToKafka(true);

        executionEnvironment
                .socketTextStream("localhost", 9999)
                .addSink(kafkaProducer);

        executionEnvironment.execute("qqqq");
    }
}