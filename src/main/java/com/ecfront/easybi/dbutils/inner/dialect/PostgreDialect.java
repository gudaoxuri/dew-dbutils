package com.ecfront.easybi.dbutils.inner.dialect;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class PostgreDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) throws SQLException {
        return sql + " limit " + pageSize + " offset " + (pageNumber - 1) * pageSize;
    }

    @Override
    public String count(String sql) throws SQLException {
        return "select count(1) from ( " + sql + " ) _" + System.currentTimeMillis();
    }

    @Override
    public String getTableInfo(String tableName) throws SQLException {
        return "SELECT * FROM pg_tables t where t.tablename='"+tableName+"'";
    }

    @Override
    public String createTableIfNotExist(String tableName, String tableDesc, Map<String, String> fields, Map<String, String> fieldsDesc, List<String> indexFields, List<String> uniqueFields, String pkField) throws SQLException {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " ( ");
        for (Map.Entry<String, String> field : fields.entrySet()) {
            String f = field.getValue().toLowerCase();
            String t;
            if (f.equals("int") || f.equals("integer") || f.equals("short")) {
                t = "integer";
            } else if (f.equals("long")) {
                t = "bigint";
            } else if (f.equals("string")) {
                t = "character varying(65535)";
            } else if (f.equals("text")) {
                t = "text";
            } else if (f.equals("bool") || f.equals("boolean")) {
                t = "boolean";
            } else if (f.equals("float") || f.equals("double")) {
                t = "double precision";
            } else if (f.equals("char")) {
                t = "character";
            } else if (f.equals("date")) {
                t = "date";
            } else if (f.equals("bigdecimal") || f.equals("decimal")) {
                t = "numeric";
            } else {
                throw new SQLException("Not support type:" + f);
            }
            sb.append(field.getKey() + " " + t + " ,");
        }
        if (uniqueFields != null && !uniqueFields.isEmpty()) {
            for (String uField : uniqueFields) {
                sb.append("CONSTRAINT \"u_" + tableName + "_" + uField + "\" UNIQUE (\"" + uField + "\"),");
            }
        }
        if (pkField != null && pkField.trim() != "") {
            sb.append("primary key(" + pkField.trim() + ") );");
        } else {
            sb = new StringBuilder(sb.substring(0, sb.length() - 1) + ");");
        }
        if (indexFields != null && !indexFields.isEmpty()) {
            for (String idxFields : indexFields) {
                sb.append("CREATE INDEX \"i_" + tableName + "_" + idxFields + "\" ON \"" + tableName + "\" (\"" + idxFields + "\");");
            }
        }
        if (tableDesc != null && !tableDesc.isEmpty()) {
            sb.append("COMMENT ON TABLE \"" + tableName + "\" IS '" + tableDesc + "';");
        }
        if (fieldsDesc != null && !fieldsDesc.isEmpty()) {
            for (Map.Entry<String, String> field : fieldsDesc.entrySet()) {
                sb.append("COMMENT ON COLUMN \"" + tableName + "\".\"" + field.getKey() + "\" IS '" + field.getValue() + "';");
            }
        }
        return sb.toString();
    }
}
