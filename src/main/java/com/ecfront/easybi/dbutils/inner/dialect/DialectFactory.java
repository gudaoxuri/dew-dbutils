package com.ecfront.easybi.dbutils.inner.dialect;


public class DialectFactory {

    public static Dialect parseDialect(String url) {
        if (url.contains("h2")) {
            return new H2Dialect();
        } else if (url.contains("mysql")) {
            return new MySQLDialect();
        } else if (url.contains("postgresql")) {
            return new PostgresDialect();
        } else if (url.contains("hive2")) {
            return new HiveDialect();
        }
        return null;
    }

}
