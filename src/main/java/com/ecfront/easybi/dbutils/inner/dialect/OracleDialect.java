package com.ecfront.easybi.dbutils.inner.dialect;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.SQLException;
import java.util.List;
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
    public String getTableInfo(String tableName) throws SQLException {
        //TODO
        throw new NotImplementedException();
    }

    @Override
    public String createTableIfNotExist(String tableName, String tableDesc, Map<String, String> fields, Map<String, String> fieldsDesc, List<String> indexFields, List<String> uniqueFields, String pkField) throws SQLException {
        //TODO
        throw new NotImplementedException();
    }


}
