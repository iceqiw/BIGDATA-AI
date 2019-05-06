package com.qiwei.flink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @author qiwei
 * @description KafkaDemo
 * @date 2019/5/6 21:10
 */
public class KafkaDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");

        //数据源配置，是一个kafka消息的消费者
        FlinkKafkaConsumer011<String> consumer =
                new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), props);
        consumer.setStartFromEarliest();
        DataStreamSource<String> s= env.addSource(consumer);
        s.print();

        env.execute("flink learning connectors kafka");
    }
}
