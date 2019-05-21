package com.qiwei.flink;

import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Vector;

/**
 * @author qiwei
 * @description KafkaDemo
 * @date 2019/5/6 21:10
 */
public class BroadCastDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        String[] NUMS = new String[]{"1", "2", "3", "4", "5"};
        String[] ZI = new String[]{"A", "B", "C"};
        DataStream<String> NumStream = env.fromElements(NUMS).broadcast();
        DataStream<String> ZiStream = env.fromElements(ZI);
        DataStream<String> MergeStream = ZiStream
                .connect(NumStream)
                .flatMap(new CoFlatMapFunction<String, String, String>() {

                    List<String> zis = new Vector<>();

                    @Override
                    public void flatMap1(String input, Collector<String> out) throws Exception {
                        out.collect(input + "----" + zis.size());
                    }

                    @Override
                    public void flatMap2(String input2, Collector<String> out) throws Exception {
                        zis.add(input2);
                    }

                }).setParallelism(2);
        MergeStream.writeAsText("test", FileSystem.WriteMode.OVERWRITE);
        env.execute("test");
    }
}
