package com.leaning.KafkaStream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {

    public static void main(String[] args) {

        String brokers = "Centos1:9092";
        String zookeepers = "Centos1:2181";

        // 定义source和sink的主题
        String from = "log";
        String to = "recommender";

        // 定义kafka的配置
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG, "logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, zookeepers);

        StreamsConfig config = new StreamsConfig(settings);

        // 构造流程拓扑器
        TopologyBuilder builder = new TopologyBuilder();

        // 定义流处理的拓扑结构
        // source
        builder.addSource("SOURCE", from)
                // Processor
                // 第二个参数为ProcessorSupplier接口类型的多态子类，重写的方法返回值是Processor对象类型，所以传入的参数可以传入Processor对象
                .addProcessor("PROCESSOR", LogProcessor::new, "SOURCE")
                .addSink("SINK", to, "PROCESSOR");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();

        System.out.println("kafka streaming start >>>>>>>>>>>>>>>>>>");
    }

}
