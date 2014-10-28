package com.ecfront.easybi.dbutils.inner.dialect;

import java.sql.SQLException;

public class HiveDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String count(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }
}
