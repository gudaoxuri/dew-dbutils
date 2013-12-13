package com.ecfront.easybi.dbutils.inner.dialect;

public class MySQLDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) {
        return sql + "limit " + (pageNumber - 1) * pageSize + ", " + pageSize;
    }
}
