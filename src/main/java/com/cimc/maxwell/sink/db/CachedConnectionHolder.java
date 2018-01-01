package com.cimc.maxwell.sink.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by 00013708 on 2017/8/16.
 */
public class CachedConnectionHolder {
	private static final Logger log = LoggerFactory.getLogger(MySqlDbWriter.class);
	private static final int DB_CONNECT_MAX_RETIRES = 3;
	private static final int DB_RETRY_BACKOFF_MS = 5000;
	private static final int CONNECTION_VALIDATE_TIMEOUT_S = 5;// 秒

	private Connection connection;

	private String username;
	private String password;
	private String url;
	private String driver;

	public CachedConnectionHolder(String username, String password, String url, String driver) {
		this.username = username;
		this.password = password;
		this.url = url;
		this.driver = driver;
	}

	// 首先尝试获取一个连接，如果连接是非法的新建一个连接，此连接不释放，作为一个Cache的连接
	public synchronized Connection getValidConnection() throws SQLException {
		if (connection != null && connection.isValid(CONNECTION_VALIDATE_TIMEOUT_S)) {
			return connection;
		} else {
			connection = createNewConnection();
			return connection;
		}
	}

	private Connection createNewConnection() throws SQLException {
		int attempts = 0;
		while (attempts < DB_CONNECT_MAX_RETIRES) {
			try {
				Class.forName(this.driver);
				Connection connection = DriverManager.getConnection(this.url, this.username, this.password);
				return connection;
			} catch (ClassNotFoundException e) {
				log.error(e.getMessage(), e);
				throw new ConnectException(e);
			} catch (SQLException e) {
				log.error(e.getMessage(), e);
				attempts++;
				if (attempts > DB_CONNECT_MAX_RETIRES) {
					throw e;
				}
				log.info("connect to database failed,has retried:{} times,retry backoff:{}", attempts,
						DB_RETRY_BACKOFF_MS);
				try {
					Thread.sleep(DB_RETRY_BACKOFF_MS);
				} catch (InterruptedException e1) {
				}
			}
		}
		return null;
	}

	public synchronized void closeQuietly() {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (SQLException e) {
			log.error(e.getMessage(), e);
		} finally {// 强制释放
			connection = null;
		}
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public String getDriver() {
		return driver;
	}

	public void setDriver(String driver) {
		this.driver = driver;
	}
}
