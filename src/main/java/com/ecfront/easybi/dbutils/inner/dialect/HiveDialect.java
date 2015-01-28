package com.ecfront.easybi.dbutils.inner.dialect;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.SQLException;
import java.util.Map;

public class HiveDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String count(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String createTableIfNotExist(String tableName, Map<String, String> fields, String pk) throws SQLException {
        throw new NotImplementedException();
    }
}
