package com.ecfront.easybi.dbutils.inner.dialect;

import java.sql.SQLException;

public class MySQLDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        return sql + " limit " + (pageNumber - 1) * pageSize + ", " + pageSize;
    }

    @Override
    public String count(String sql) throws SQLException {
        return "select count(1) from ( "+sql+" ) _"+ System.currentTimeMillis();
    }
}
