package com.leaning.KafkaStream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[], byte[]> {

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext processorContext) {
        this.context = context;
    }

    @Override
    public void process(byte[] logKey, byte[] line) {
        // 处理日志每一行数据
        String input = new String(line);
        // 通过制定前缀过滤掉无用的日志数据
        if(input.contains("MOVIE_RATING_PREFIX:")){
            System.out.println("movie rating coming!!!!" + input);
            input = input.split("MOVIE_RATING_PREFIX")[1].trim();
            // 发送
            context.forward("logProcessor".getBytes(), input.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
