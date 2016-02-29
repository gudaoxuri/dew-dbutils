package com.ecfront.easybi.dbutils.inner.dialect;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

class HiveDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String count(String sql) throws SQLException {
        throw new SQLException("Method not supported");
    }

    @Override
    public String getTableInfo(String tableName) throws SQLException {
        //TODO
        throw new NotImplementedException();
    }

    @Override
    public String createTableIfNotExist(String tableName, String tableDesc, Map<String, String> fields, Map<String, String> fieldsDesc, List<String> indexFields, List<String> uniqueFields, String pkField) throws SQLException {
        //TODO
        throw new NotImplementedException();
    }

    @Override
    public String getDriver() {
        return "org.apache.hive.jdbc.HiveDriver";
    }

    @Override
    public DialectType getDialectType() {
        return DialectType.HIVE;
    }
}
