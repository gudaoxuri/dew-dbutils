package com.ecfront.easybi.dbutils.inner;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

public class Transaction {


    public static Connection open(String dsCode) {
        Connection conn = threadLocalConnection.get();
        try {
            if (null == conn) {
                conn = DSLoader.getConnection(dsCode);
                threadLocalConnection.set(conn);
            }
            conn.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error("Connection open error.", e);
        }
        return conn;
    }

    public static void commit() {
        Connection conn = threadLocalConnection.get();
        if (null != conn) {
            try {
                conn.commit();
            } catch (SQLException e) {
                logger.error("Connection commit error.", e);
            }
        }
        close();
    }

    public static void rollback() {
        Connection conn = threadLocalConnection.get();
        if (null != conn) {
            try {
                conn.rollback();
            } catch (SQLException e) {
                logger.error("Connection rollback error.", e);
            }
        }
        close();
    }

    private static void close() {
        Connection conn = threadLocalConnection.get();
        if (null != conn) {
            try {
                conn.close();
                threadLocalConnection.set(null);
            } catch (SQLException e) {
                logger.error("Connection close error.", e);
            }
        }
    }

    private static final ThreadLocal<Connection> threadLocalConnection = new ThreadLocal<Connection>();

    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);
}
