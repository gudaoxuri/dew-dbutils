package com.ecfront.easybi.dbutils.inner.dbutilsext;

import com.ecfront.easybi.dbutils.inner.ConnectionWrap;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.ResultSetHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class QueryRunnerExt extends QueryRunner {

    public int[] batch(ConnectionWrap cw, String sql, Object[][] params) throws SQLException {
        return super.batch(cw.conn, sql, params);
    }


    public <T> T query(ConnectionWrap cw, String sql, ResultSetHandler<T> rsh, Object... params) throws SQLException {
        return super.query(cw.conn, sql, rsh, params);
    }


    public <T> T query(ConnectionWrap cw, String sql, ResultSetHandler<T> rsh) throws SQLException {
        return querySimple(cw.conn, sql, rsh);
    }

    public int update(ConnectionWrap cw, String sql) throws SQLException {
        return super.update(cw.conn, sql);
    }

    public int update(ConnectionWrap cw, String sql, Object[] param) throws SQLException {
        return super.update(cw.conn, sql, param);
    }

    protected <T> T querySimple(Connection conn, String sql, ResultSetHandler<T> rsh) throws SQLException {
        if (conn == null) {
            throw new SQLException("Null connection");
        }
        if (sql == null) {
            throw new SQLException("Null SQL statement");
        }
        if (rsh == null) {
            throw new SQLException("Null ResultSetHandler");
        }
        Statement st = null;
        ResultSet rs = null;
        T result = null;
        try {
            logger.debug("Executing:" + sql);
            st = conn.createStatement();
            rs = this.wrap(st.executeQuery(sql));
            result = rsh.handle(rs);
            logger.debug("Executed:" + sql);
        } catch (SQLException e) {
            this.rethrow(e, sql);
        } finally {
            try {
                close(rs);
            } finally {
                close(st);
            }
        }
        return result;
    }

    private static final Logger logger = LoggerFactory.getLogger(QueryRunnerExt.class);
}
