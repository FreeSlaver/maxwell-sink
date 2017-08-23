/*
 * Copyright 2016 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cimc.maxwell.sink;

import com.cimc.maxwell.sink.db.MySqlDbWriter;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Collection;
import java.util.Map;

public final class MySqlSinkTask extends SinkTask {
    private static final Logger log = LoggerFactory.getLogger(MySqlSinkTask.class);

    private int remainRetries = 0;

    private MySqlSinkConfig config;
    private MySqlDbWriter writer;

    @Override
    public void start(final Map<String, String> props) {
        log.info("Starting MySqlSinkTask");

        config = new MySqlSinkConfig(props);
        writer = new MySqlDbWriter(config);
        remainRetries = config.maxRetries;
    }

    @Override
    public void put(final Collection<SinkRecord> records) {
        if (records.isEmpty()) {
            return;
        }
        log.info("===>>>records size:{}", records.size());
        try {
            writer.batchWrite(records);
        } catch (SQLException e) {
            log.error(e.getMessage(), e);
            if (remainRetries <= 0) {
                throw new ConnectException(e);
            } else {
                writer.closeQuietly();
                writer = new MySqlDbWriter(config);
                remainRetries--;
                context.timeout(config.retryBackoffMs);
                throw new RetriableException(e);
            }
        }
        remainRetries = config.maxRetries;
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
        // Not necessary
    }

    public synchronized void stop() {
        log.info("Stopping task");
        writer.closeQuietly();
    }


    public String version() {
        return "1.0.0";
    }

}
