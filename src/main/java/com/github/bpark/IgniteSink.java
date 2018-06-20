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
package com.github.bpark;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.ignite.*;

import java.util.Map;

/**
 * Apache Flink Ignite sink implemented as a RichSinkFunction.
 */
public class IgniteSink<IN> extends RichSinkFunction<IN> {
    /** Default flush frequency. */
    private static final long DFLT_FLUSH_FREQ = 10000L;

    /** Logger. */
    private final transient IgniteLogger log;

    /** Automatic flush frequency. */
    private long autoFlushFrequency = DFLT_FLUSH_FREQ;

    /** Enables overwriting existing values in cache. */
    private boolean allowOverwrite = false;

    /** Flag for stopped state. */
    private static volatile boolean stopped = true;

    /**
     * Obtains data flush frequency.
     *
     * @return Flush frequency.
     */
    public long getAutoFlushFrequency() {
        return autoFlushFrequency;
    }

    /**
     * Specifies data flush frequency into the grid.
     *
     * @param autoFlushFrequency Flush frequency.
     */
    public void setAutoFlushFrequency(long autoFlushFrequency) {
        this.autoFlushFrequency = autoFlushFrequency;
    }

    /**
     * Obtains flag for enabling overwriting existing values in cache.
     *
     * @return True if overwriting is allowed, false otherwise.
     */
    public boolean getAllowOverwrite() {
        return allowOverwrite;
    }

    /**
     * Enables overwriting existing values in cache.
     *
     * @param allowOverwrite Flag value.
     */
    public void setAllowOverwrite(boolean allowOverwrite) {
        this.allowOverwrite = allowOverwrite;
    }

    /**
     * Default IgniteSink constructor.
     */
    public IgniteSink() {
        this.log = SinkContext.INSTANCE.getIgnite().log();
    }

    /**
     * Starts streamer.
     *
     * @throws IgniteException If failed.
     */
    @SuppressWarnings("unchecked")
    public void start() throws IgniteException {

        SinkContext.INSTANCE.getStreamer().autoFlushFrequency(autoFlushFrequency);
        SinkContext.INSTANCE.getStreamer().allowOverwrite(allowOverwrite);

        stopped = false;
    }

    /**
     * Stops streamer.
     *
     * @throws IgniteException If failed.
     */
    public void stop() throws IgniteException {
        if (stopped)
            return;

        stopped = true;

        SinkContext.INSTANCE.getStreamer().close();
        SinkContext.INSTANCE.getIgnite().cache("DemoCache").close();
        SinkContext.INSTANCE.getIgnite().close();
    }

    /**
     * Transfers data into grid. It is called when new data
     * arrives to the sink, and forwards it to {@link IgniteDataStreamer}.
     *
     * @param in IN.
     */
    @SuppressWarnings("unchecked")
    @Override
    public void invoke(IN in) {
        try {
            if (!(in instanceof Map))
                throw new IgniteException("Map as a streamer input is expected!");

            SinkContext.INSTANCE.getStreamer().addData((Map)in);
        }
        catch (Exception e) {
            log.error("Error while processing IN of DemoCache", e);
        }
    }

    private enum SinkContext {
        INSTANCE;

        private Ignite ignite;
        private IgniteDataStreamer streamer;

        SinkContext() {
            ignite = Ignition.start("flink-config.xml");
            streamer = ignite.dataStreamer("DemoCache");
        }

        /**
         * Obtains grid instance.
         *
         * @return Grid instance.
         */
        private Ignite getIgnite() {
            return ignite;
        }

        /**
         * Obtains data streamer instance.
         *
         * @return Data streamer instance.
         */
        private IgniteDataStreamer getStreamer() {
            return streamer;
        }
    }
}

