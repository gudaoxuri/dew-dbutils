package com.ecfront.easybi.dbutils.inner.dialect;

import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class H2Dialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        return sql + " limit " + pageSize + " offset " + (pageNumber - 1) * pageSize;
    }

    @Override
    public String count(String sql) throws SQLException {
        return "select count(1) from ( " + sql + " ) ";
    }

    @Override
    public String getTableInfo(String tableName) throws SQLException {
        //TODO
        throw new NotImplementedException();
    }

    @Override
    public String createTableIfNotExist(String tableName, String tableDesc, Map<String, String> fields, Map<String, String> fieldsDesc, List<String> indexFields, List<String> uniqueFields, String pkField) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " ( ");
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String f = field.getValue().toLowerCase();
            String t;
            if (f.equals("int") || f.equals("integer")) {
                t = "INT";
            } else if (f.equals("long")) {
                t = "BIGINT";
            } else if (f.equals("short")) {
                t = "SMALLINT";
            } else if (f.equals("string")) {
                t = "VARCHAR(65535)";
            } else if (f.equals("bool") || f.equals("boolean")) {
                t = "BOOLEAN";
            } else if (f.equals("float")) {
                t = "FLOAT";
            } else if (f.equals("double")) {
                t = "DOUBLE";
            } else if (f.equals("char")) {
                t = "CHAR";
            } else if (f.equals("date")) {
                t = "TIMESTAMP";
            } else if (f.equals("uuid")) {
                t = "UUID";
            } else if (f.equals("decimal")) {
                t = "DECIMAL";
            } else {
                throw new SQLException("Not support type:" + f);
            }
            sb.append(field.getKey() + " " + t + " ,");
        }
        if (pkField != null && pkField.trim() != "") {
            return sb.append("primary key(" + pkField.trim() + ") )").toString();
        } else {
            return sb.substring(0, sb.length() - 1).toString()+")";
        }
        //TODO
    }
}
