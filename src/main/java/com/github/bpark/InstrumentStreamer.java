package com.github.bpark;

/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.ignite.sink.flink.IgniteSink;

import java.util.HashMap;
import java.util.Map;

/**
 * Demo Streamer with Ignite Sink.
 *
 * Example message: "0;XYZ;12;12;13;13\n"
 */
public class InstrumentStreamer {

    private static final String ID_TMPL = "%s##%s";

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().enableSysoutLogging();

        IgniteSink<Map<String, String>> igniteSink = new IgniteSink<>("DemoCache", "flink-config.xml");
        igniteSink.setAllowOverwrite(true);
        igniteSink.setAutoFlushFrequency(10);

        igniteSink.start();

        DataStream<String> text = env.socketTextStream("localhost", 12200);

        DataStream<Map<String, String>> datastream = text.flatMap(new Splitter());
        datastream.addSink(igniteSink);

        env.execute("Demo Streamer");
        igniteSink.stop();
    }

    public static final class Splitter implements FlatMapFunction<String, Map<String, String>> {

        @Override
        public void flatMap(String value, Collector<Map<String, String>> collector) throws Exception {
            String[] tokens = value.split(";");
            Map<String, String> data = new HashMap<>();
            if (tokens.length > 1) {
                String id = tokens[1];
                if (id != null) {
                    for (int i = 2; i < tokens.length - 1; i += 2) {
                        if (tokens[i] != null && tokens[i + 1] != null) {
                            data.put(String.format(ID_TMPL, id, tokens[i]), tokens[i + 1]);
                        }
                    }
                }
            }
            if (data.size() > 0) {
                collector.collect(data);
            }
        }
    }

}
