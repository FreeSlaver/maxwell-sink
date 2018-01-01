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

import java.util.Map;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

public class MySqlSinkConfig extends AbstractConfig {

	public final String topics;
	public final Integer maxRetries;
	public final Integer retryBackoffMs;

	// db config
	public final String mysqlDriver;
	public final String mysqlUsername;
	public final String mysqlPassword;
	public final String mysqlUrl;

	// dbtables
	public final String dbtables;
	// filter conditions
	public final String fieldFilters;
	// dml filters
	public final String dmlFilters;

	public MySqlSinkConfig(Map<?, ?> props) {
		super(CONFIG_DEF, props);
		this.topics = getString(TOPICS);
		this.maxRetries = getInt(MAX_RETRIES);
		this.retryBackoffMs = getInt(RETRY_BACKOFF_MS);

		this.mysqlDriver = getString(MYSQL_DRIVER);
		this.mysqlUsername = getString(MYSQL_USERNAME);
		this.mysqlPassword = getString(MYSQL_PASSWORD);
		this.mysqlUrl = getString(MYSQL_URL);

		this.dbtables = getString(DBTABLES);
		this.fieldFilters = getString(FIELD_FILTERS);
		this.dmlFilters = getString(DML_FILTERS);
	}

	public static final String TOPICS = "topics";
	public static final String MAX_RETRIES = "max.retries";
	public static final String RETRY_BACKOFF_MS = "retry.backoff.ms";

	public static final String MYSQL_DRIVER = "mysql.driver";
	public static final String MYSQL_USERNAME = "mysql.username";
	public static final String MYSQL_PASSWORD = "mysql.password";
	public static final String MYSQL_URL = "mysql.url";

	public static final String DBTABLES = "dbtables";

	public static final String FIELD_FILTERS = "field.filters";

	public static final String DML_FILTERS = "dml.filters";

	public static ConfigDef CONFIG_DEF = new ConfigDef()
			.define(TOPICS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "topics")
			.define(MAX_RETRIES, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "max retries")
			.define(RETRY_BACKOFF_MS, ConfigDef.Type.INT, ConfigDef.Importance.HIGH, "retry backoff ms")

	.define(MYSQL_DRIVER, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql driver class name")
			.define(MYSQL_USERNAME, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql username")
			.define(MYSQL_PASSWORD, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql password")
			.define(MYSQL_URL, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "mysql url")

	.define(DBTABLES, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "dbtables")
			.define(FIELD_FILTERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "field filters")
			.define(DML_FILTERS, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH, "dml filters");

}
