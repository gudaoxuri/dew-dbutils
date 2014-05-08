package com.ecfront.easybi.dbutils.inner;

import com.alibaba.druid.pool.DruidDataSource;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import com.ecfront.easybi.dbutils.inner.dialect.H2Dialect;
import com.ecfront.easybi.dbutils.inner.dialect.MySQLDialect;
import com.ecfront.easybi.dbutils.inner.dialect.OracleDialect;
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
        MULTI_DS.clear();
        MULTI_DB_DIALECT.clear();
        loadMainDS();
        if (ConfigContainer.IS_MULTI_DS_SUPPORT) {
            loadMultiDS();
        }
    }

    public static Connection getConnection(String dsCode) {
        try {
            Connection connection = MULTI_DS.get(dsCode).getConnection();
            logger.debug("Connection info:" + connection.toString() + ",isClosed:" + connection.isClosed());
            return connection;
        } catch (SQLException e) {
            logger.error("Connection get error.", e);
        }
        return null;
    }

    public static Dialect getDialect(String dsCode) {
        return MULTI_DB_DIALECT.get(dsCode);
    }

    private static void loadMultiDS() {
        List<Map<String, Object>> result = null;
        try {
            result = DBExecutor.find(ConfigContainer.MULTI_DS_QUERY, null, getConnection(null), true);
        } catch (Exception e) {
            logger.error("Multi DS load error : " + e);
        }
        if (null != result) {
            for (Map<String, Object> res : result) {
                if (null != res) {
                    DruidDataSource ds = new DruidDataSource();
                    ds.setUrl(res.get(ConfigContainer.FLAG_URL.toUpperCase()).toString());
                    ds.setDriverClassName(res.get(ConfigContainer.FLAG_DRIVER.toUpperCase()).toString());
                    ds.setUsername(res.get(ConfigContainer.FLAG_USERNAME.toUpperCase()).toString());
                    ds.setPassword(res.get(ConfigContainer.FLAG_PASSWORD.toUpperCase()).toString());
                    ds.setDefaultAutoCommit(Boolean.valueOf(res.get(ConfigContainer.FLAG_DEFAULT_AUTO_COMMIT.toUpperCase()).toString()));
                    ds.setInitialSize(Integer.valueOf(res.get(ConfigContainer.FLAG_INITIAL_SIZE.toUpperCase()).toString()));
                    ds.setMaxActive(Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_ACTIVE.toUpperCase()).toString()));
                    ds.setMinIdle(Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_IDLE.toUpperCase()).toString()));
                    ds.setMaxIdle(Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_IDLE.toUpperCase()).toString()));
                    ds.setMaxWait(Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_WAIT.toUpperCase()).toString()));
                    ds.setRemoveAbandoned(Boolean.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED.toUpperCase()).toString()));
                    ds.setRemoveAbandonedTimeoutMillis(Integer.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED_TIMEOUT.toUpperCase()).toString()));
                    ds.setTimeBetweenEvictionRunsMillis(Integer.valueOf(res.get(ConfigContainer.FLAG_TIME_BETWEEN_EVICTION_RUMS.toUpperCase()).toString()));
                    ds.setMinEvictableIdleTimeMillis(Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_EVICTABLE_IDLE_TIME.toUpperCase()).toString()));
                    MULTI_DS.put(res.get(ConfigContainer.FLAG_CODE.toUpperCase()).toString(), ds);
                    MULTI_DB_DIALECT.put(res.get(ConfigContainer.FLAG_CODE.toUpperCase()).toString(), parseDialect(res.get(ConfigContainer.FLAG_URL).toString()));
                }
            }
        }
    }

    private static void loadMainDS() {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(ConfigContainer.DB_JDBC_URL);
        ds.setDriverClassName(ConfigContainer.DB_JDBC_DRIVER);
        ds.setUsername(ConfigContainer.DB_JDBC_USERNAME);
        ds.setPassword(ConfigContainer.DB_JDBC_PASSWORD);
        ds.setDefaultAutoCommit(ConfigContainer.DB_POOL_DEFAULT_AUTO_COMMIT);
        ds.setInitialSize(ConfigContainer.DB_POOL_INITIAL_SIZE);
        ds.setMaxActive(ConfigContainer.DB_POOL_MAX_ACTIVE);
        ds.setMinIdle(ConfigContainer.DB_POOL_MIN_IDLE);
        ds.setMaxIdle(ConfigContainer.DB_POOL_MAX_IDLE);
        ds.setMaxWait(ConfigContainer.DB_POOL_MAX_WAIT);
        ds.setRemoveAbandoned(ConfigContainer.DB_POOL_REMOVE_ABANDONED);
        ds.setRemoveAbandonedTimeoutMillis(ConfigContainer.DB_POOL_REMOVE_ABANDONED_TIMEOUT);
        ds.setTimeBetweenEvictionRunsMillis(ConfigContainer.DB_POOL_TIME_BETWEEN_EVICTION_RUMS);
        ds.setMinEvictableIdleTimeMillis(ConfigContainer.DB_POOL_MIN_EVICTABLE_IDLE_TIME);
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
        reload();
    }


}
