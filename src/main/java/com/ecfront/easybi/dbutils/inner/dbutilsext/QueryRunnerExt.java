package com.ecfront.easybi.dbutils.inner.dbutilsext;

import org.apache.commons.dbutils.QueryRunner;

/**
 * Created by 震宇 on 2014/5/10.
 */
public class QueryRunnerExt extends QueryRunner {

    /*public Long count(Connection conn, String sql, Object... params) throws SQLException {
        if (conn == null) {
            throw new SQLException("Null connection");
        }
        if (sql == null) {
            throw new SQLException("Null SQL statement");
        }
        PreparedStatement stmt = null;
        ResultSet rs = null;
        try {
            stmt = conn.prepareStatement(sql);
            fillStatement(stmt, params);
            rs = stmt.executeQuery();
            if (rs.next()) {
                Object result = rs.getObject(1);
                if (result instanceof BigDecimal) {
                    return ((BigDecimal) result).longValue();
                } else if (result instanceof Long) {
                    return (Long) result;
                } else {
                    return ((Number) result).longValue();
                }
            } else {
                return null;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (rs != null) {
                    rs.close();
                }
            } finally {
                if (stmt != null) {
                    stmt.close();
                }
            }
        }
        return null;
    }*/
}
