package com.ecfront.easybi.dbutils.inner.dialect;

public class H2Dialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) {
        return sql + " limit " + pageSize + " offset " + (pageNumber - 1) * pageSize;
    }

    @Override
    public String count(String sql) {
        return "select count(1) from ( "+sql+" ) ";
    }
}
