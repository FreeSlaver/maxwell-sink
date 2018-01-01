package com.cimc.sink.db;

import com.cimc.maxwell.sink.db.CachedConnectionHolder;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by 00013708 on 2017/8/23.
 */
public class CachedConnectionHolderTest {

    @Test
    public void getValidConnectionTest() throws SQLException {
        String username = "estation";
        String password = "sdnnysqt@lb";
        String url = "jdbc:mysql://119.29.214.125:3306?autoReconnect=true&characterEncoding=utf8&allowMultiQueries=true";
        String driver = "com.mysql.jdbc.Driver";
        CachedConnectionHolder holder = new CachedConnectionHolder(username, password, url, driver);

        Connection conn = holder.getValidConnection();
        Assert.assertTrue(conn != null && conn.isValid(5));

        holder.closeQuietly();
        Assert.assertTrue(conn == null || !conn.isValid(5));
    }
}
