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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class MySqlSinkConnector extends SinkConnector {
	private static final Logger log = LoggerFactory.getLogger(MySqlSinkConnector.class);

	private static final ConfigDef CONFIG_DEF = MySqlSinkConfig.CONFIG_DEF;

	private Map<String, String> configProps;

	public Class<? extends Task> taskClass() {
		return MySqlSinkTask.class;
	}

	@Override
	public List<Map<String, String>> taskConfigs(int maxTasks) {
		log.info("Setting task configurations for {} workers.", maxTasks);
		final List<Map<String, String>> configs = new ArrayList<Map<String, String>>(maxTasks);
		for (int i = 0; i < maxTasks; ++i) {
			configs.add(configProps);
		}
		return configs;
	}

	@Override
	public void start(Map<String, String> props) {
		configProps = props;
	}

	@Override
	public void stop() {

	}

	@Override
	public ConfigDef config() {
		return CONFIG_DEF;
	}

	@Override
	public String version() {
		return "1.0.0";
	}
}
