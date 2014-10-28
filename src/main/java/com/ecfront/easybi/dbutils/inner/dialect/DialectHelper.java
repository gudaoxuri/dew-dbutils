package com.ecfront.easybi.dbutils.inner.dialect;

/**
 * Created by sunisle on 2014/10/28.
 */
public class DialectHelper {

    public static Dialect parseDialect(String driver) {
        DialectType type = getDialectType(driver);
        switch (type) {
            case ORACLE:
                return new OracleDialect();
            case H2:
                return new H2Dialect();
            case MYSQL:
                return new MySQLDialect();
            case SPARK_SQL:
                return new HiveDialect();
        }
        return null;
    }

    public static DialectType getDialectType(String driver) {
        if (driver.indexOf("OracleDriver") != -1) {
            return DialectType.ORACLE;
        } else if (driver.indexOf("h2") != -1) {
            return DialectType.H2;
        } else if (driver.indexOf("mysql") != -1) {
            return DialectType.MYSQL;
        } else if (driver.indexOf("HiveDriver") != -1) {
            return DialectType.SPARK_SQL;
        }
        return null;
    }
}
