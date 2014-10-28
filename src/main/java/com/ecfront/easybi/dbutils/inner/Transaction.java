package com.ecfront.easybi.dbutils.inner;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class Transaction {


    public static ConnectionWrap open(String dsCode) {
        ConnectionWrap cw = threadLocalConnection.get();
        try {
            if (null == cw) {
                cw = DSLoader.getConnection(dsCode);
                threadLocalConnection.set(cw);
            }
            cw.conn.setAutoCommit(false);
        } catch (SQLException e) {
            logger.error("Connection open error.", e);
        }
        return cw;
    }

    public static void commit() {
        ConnectionWrap cw = threadLocalConnection.get();
        if (null != cw.conn) {
            try {
                cw.conn.commit();
            } catch (SQLException e) {
                logger.error("Connection commit error.", e);
            }
        }
        close();
    }

    public static void rollback() {
        ConnectionWrap cw = threadLocalConnection.get();
        if (null != cw.conn) {
            try {
                cw.conn.rollback();
            } catch (SQLException e) {
                logger.error("Connection rollback error.", e);
            }
        }
        close();
    }

    private static void close() {
        ConnectionWrap cw = threadLocalConnection.get();
        if (null != cw.conn) {
            try {
                cw.conn.close();
                threadLocalConnection.set(null);
            } catch (SQLException e) {
                logger.error("Connection close error.", e);
            }
        }
    }

    private static final ThreadLocal<ConnectionWrap> threadLocalConnection = new ThreadLocal<ConnectionWrap>();

    private static final Logger logger = LoggerFactory.getLogger(Transaction.class);
}
