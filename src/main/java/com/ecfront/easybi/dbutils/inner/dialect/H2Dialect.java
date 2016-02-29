package com.ecfront.easybi.dbutils.inner.dialect;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

class H2Dialect implements Dialect {

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
        return "SELECT * FROM INFORMATION_SCHEMA.TABLES t where t.table_name ='" + tableName + "'";
    }

    @Override
    public String createTableIfNotExist(String tableName, String tableDesc, Map<String, String> fields, Map<String, String> fieldsDesc, List<String> indexFields, List<String> uniqueFields, String pkField) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " ( ");
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String f = field.getValue().toLowerCase();
            String t;
            switch (f) {
                case "int":
                case "integer":
                    t = "INT";
                    break;
                case "long":
                    t = "BIGINT";
                    break;
                case "short":
                    t = "SMALLINT";
                    break;
                case "string":
                    t = "VARCHAR(65535)";
                    break;
                case "text":
                    t = "CLOB";
                    break;
                case "bool":
                case "boolean":
                    t = "BOOLEAN";
                    break;
                case "float":
                    t = "FLOAT";
                    break;
                case "double":
                    t = "DOUBLE";
                    break;
                case "char":
                    t = "CHAR";
                    break;
                case "date":
                    t = "TIMESTAMP";
                    break;
                case "uuid":
                    t = "UUID";
                    break;
                case "bigdecimal":
                case "decimal":
                    t = "DECIMAL";
                    break;
                default:
                    throw new SQLException("Not support type:" + f);
            }
            sb.append(field.getKey()).append(" ").append(t).append(" ,");
        }
        if (pkField != null && !Objects.equals(pkField.trim(), "")) {
            return sb.append("primary key(").append(pkField.trim()).append(") )").toString();
        } else {
            return sb.substring(0, sb.length() - 1) + ")";
        }
        //TODO
    }

    @Override
    public String getDriver() {
        return "org.h2.Driver";
    }

    @Override
    public DialectType getDialectType() {
        return DialectType.H2;
    }

}
