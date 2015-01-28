package com.ecfront.easybi.dbutils.inner.dialect;

import java.sql.SQLException;
import java.util.Map;

public interface Dialect {

    String paging(String sql, long pageNumber, long pageSize) throws SQLException;

    String count(String sql) throws SQLException;

    String createTableIfNotExist(String tableName,Map<String,String> fields,String pk)throws SQLException;
}
