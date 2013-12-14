package com.ecfront.easybi.dbutils.inner;

import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import com.ecfront.easybi.dbutils.inner.dialect.H2Dialect;
import com.ecfront.easybi.dbutils.inner.dialect.MySQLDialect;
import com.ecfront.easybi.dbutils.inner.dialect.OracleDialect;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DSLoader {

    private static final Map<String, DataSource> MULTI_DS = new HashMap<String, DataSource>();
    private static final Map<String, Dialect> MULTI_DB_DIALECT = new HashMap<String, Dialect>();

    public static void reload() {
        if (ConfigContainer.IS_MULTI_DS_SUPPORT) {
            loadMultiDS();
        }
    }

    public static Connection getConnection(String dsCode) {
        try {
            return MULTI_DS.get(dsCode).getConnection();
        } catch (SQLException e) {
            logger.error("Connection get error.", e);
        }
        return null;
    }

    public static Dialect getDialect(String dsCode) {
        return MULTI_DB_DIALECT.get(dsCode);
    }

    private static void loadMultiDS() {
        List<Map<String, Object>> result = DBExecutor.find(ConfigContainer.MULTI_DS_QUERY, null, getConnection(null));
        if (null != result) {
            for (Map<String, Object> res : result) {
                if (null != res) {
                    BasicDataSource ds = new BasicDataSource();
                    ds.setUrl(res.get(ConfigContainer.FLAG_URL.toUpperCase()).toString());
                    ds.setDriverClassName(res.get(ConfigContainer.FLAG_DRIVER.toUpperCase()).toString());
                    ds.setUsername(res.get(ConfigContainer.FLAG_USERNAME.toUpperCase()).toString());
                    ds.setPassword(res.get(ConfigContainer.FLAG_PASSWORD.toUpperCase()).toString());
                    ds.setDefaultAutoCommit(true);
                    ds.setInitialSize(Integer.valueOf(res.get(ConfigContainer.FLAG_INITIAL_SIZE.toUpperCase()).toString()));
                    ds.setMaxActive(Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_ACTIVE.toUpperCase()).toString()));
                    ds.setMinIdle(Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_IDLE.toUpperCase()).toString()));
                    ds.setMaxIdle(Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_IDLE.toUpperCase()).toString()));
                    ds.setMaxWait(Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_WAIT.toUpperCase()).toString()));
                    MULTI_DS.put(res.get(ConfigContainer.FLAG_CODE.toUpperCase()).toString(), ds);
                    MULTI_DB_DIALECT.put(res.get(ConfigContainer.FLAG_CODE.toUpperCase()).toString(), parseDialect(res.get(ConfigContainer.FLAG_URL).toString()));
                }
            }
        }
    }

    private static void loadMainDS() {
        BasicDataSource ds = new BasicDataSource();
        ds.setUrl(ConfigContainer.DB_JDBC_URL);
        ds.setDriverClassName(ConfigContainer.DB_JDBC_DRIVER);
        ds.setUsername(ConfigContainer.DB_JDBC_USERNAME);
        ds.setPassword(ConfigContainer.DB_JDBC_PASSWORD);
        ds.setDefaultAutoCommit(true);
        ds.setInitialSize(ConfigContainer.DB_POOL_INITIAL_SIZE);
        ds.setMaxActive(ConfigContainer.DB_POOL_MAX_ACTIVE);
        ds.setMinIdle(ConfigContainer.DB_POOL_MIN_IDLE);
        ds.setMaxIdle(ConfigContainer.DB_POOL_MAX_IDLE);
        ds.setMaxWait(ConfigContainer.DB_POOL_MAX_WAIT);
        MULTI_DS.put(null, ds);
        MULTI_DB_DIALECT.put(null, parseDialect(ConfigContainer.DB_JDBC_URL));
    }

    private static Dialect parseDialect(String driver) {
        if (null != driver && driver.split(":").length > 2) {
            String type = driver.split(":")[1].trim();
            if ("oracle".equalsIgnoreCase(type)) {
                return new OracleDialect();
            } else if ("mysql".equalsIgnoreCase(type)) {
                return new MySQLDialect();
            } else if ("h2".equalsIgnoreCase(type)) {
                return new H2Dialect();
            }
        }
        logger.error("Parse dialect error : " + driver);
        return null;
    }

    private static final Logger logger = LoggerFactory.getLogger(DSLoader.class);

    static {
        loadMainDS();
        if (ConfigContainer.IS_MULTI_DS_SUPPORT) {
            loadMultiDS();
        }
    }


}
