package com.ecfront.easybi.dbutils.inner.dialect;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.SQLException;
import java.util.Map;

public class OracleDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        return "select * from (select rownum rn , originaltable.* from ("+ sql + ") originaltable  where rownum<=" + (pageNumber * pageSize) + ") where rn > " + (pageNumber - 1) * pageSize;
    }

    @Override
    public String count(String sql) throws SQLException {
        return "select count(1) from ( "+sql+" ) ";
    }

    @Override
    public String createTableIfNotExist(String tableName, Map<String, String> fields, String pk) throws SQLException {
        throw new NotImplementedException();
    }


}
