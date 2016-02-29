package com.ecfront.easybi.dbutils.inner.dbutilsext;

import com.ecfront.easybi.dbutils.inner.ConnectionWrap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;

public class QueryRunnerExt extends QueryRunner {

    public int[] batch(ConnectionWrap cw, String sql, Object[][] params) throws SQLException {
        return super.batch(cw.conn, sql, params);
    }


    public <T> T query(ConnectionWrap cw, String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
        return super.query(cw.conn, sql, rsh, params);
    }


    public <T> T query(ConnectionWrap cw, String sql, ResultSetHandler<T> rsh) throws SQLException {
        return super.query(cw.conn, sql, rsh);
    }

    public int update(ConnectionWrap cw, String sql) throws SQLException {
        return super.update(cw.conn, sql);
    }

    public int update(ConnectionWrap cw, String sql, Object[] param) throws SQLException {
        return super.update(cw.conn, sql, param);
    }

    private static final Logger logger = LoggerFactory.getLogger(QueryRunnerExt.class);
}
