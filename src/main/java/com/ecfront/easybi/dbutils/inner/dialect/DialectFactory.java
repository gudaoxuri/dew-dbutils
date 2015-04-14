package com.ecfront.easybi.dbutils.inner.dialect;


public class DialectFactory {

    public static Dialect parseDialect(String driver) {
        DialectType type = getDialectType(driver);
        assert type != null;
        switch (type) {
            case ORACLE:
                return new OracleDialect();
            case H2:
                return new H2Dialect();
            case MYSQL:
                return new MySQLDialect();
            case POSTGRE:
                return new PostgresDialect();
            case SPARK_SQL:
                return new HiveDialect();
        }
        return null;
    }

    public static DialectType getDialectType(String driver) {
        if (driver.contains("OracleDriver")) {
            return DialectType.ORACLE;
        } else if (driver.contains("h2")) {
            return DialectType.H2;
        } else if (driver.contains("mysql")) {
            return DialectType.MYSQL;
        } else if (driver.contains("postgresql")) {
            return DialectType.POSTGRE;
        } else if (driver.contains("HiveDriver")) {
            return DialectType.SPARK_SQL;
        }
        return null;
    }
}
