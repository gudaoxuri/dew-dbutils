package com.ecfront.easybi.dbutils.inner.dialect;

import java.sql.SQLException;

public class H2Dialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        return sql + " limit " + pageSize + " offset " + (pageNumber - 1) * pageSize;
    }

    @Override
    public String count(String sql) throws SQLException {
        return "select count(1) from ( "+sql+" ) ";
    }
}
