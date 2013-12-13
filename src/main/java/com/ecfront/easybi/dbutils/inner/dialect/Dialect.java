package com.ecfront.easybi.dbutils.inner.dialect;

public interface Dialect {

    String paging(String sql, long pageNumber, long pageSize);
}
