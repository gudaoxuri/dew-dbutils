package com.ecfront.easybi.dbutils.inner.dialect;

import java.sql.SQLException;

public interface Dialect {

    String paging(String sql, long pageNumber, long pageSize) throws SQLException;

    String count(String sql) throws SQLException;
}
