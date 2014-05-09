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
    private static final Map<String, PoolDTO> MULTI_POOL = new HashMap<String, PoolDTO>();

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
            if (connection.isClosed()) {
                //Re-setting connection when connection was close.
                loadPool(MULTI_POOL.get(dsCode));
                connection = MULTI_DS.get(dsCode).getConnection();
            }
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
                    PoolDTO pool = new PoolDTO();
                    pool.flag = res.get(ConfigContainer.FLAG_CODE.toUpperCase()).toString();
                    pool.url = res.get(ConfigContainer.FLAG_URL.toUpperCase()).toString();
                    pool.driver = res.get(ConfigContainer.FLAG_DRIVER.toUpperCase()).toString();
                    pool.userName = res.get(ConfigContainer.FLAG_USERNAME.toUpperCase()).toString();
                    pool.password = res.get(ConfigContainer.FLAG_PASSWORD.toUpperCase()).toString();
                    pool.defaultAutoCommit = Boolean.valueOf(res.get(ConfigContainer.FLAG_DEFAULT_AUTO_COMMIT.toUpperCase()).toString());
                    pool.initialSize = Integer.valueOf(res.get(ConfigContainer.FLAG_INITIAL_SIZE.toUpperCase()).toString());
                    pool.maxActive = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_ACTIVE.toUpperCase()).toString());
                    pool.minIdle = Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_IDLE.toUpperCase()).toString());
                    pool.maxIdle = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_IDLE.toUpperCase()).toString());
                    pool.maxWait = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_WAIT.toUpperCase()).toString());
                    pool.removeAbandoned = Boolean.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED.toUpperCase()).toString());
                    pool.removeAbandonedTimeoutMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED_TIMEOUT.toUpperCase()).toString());
                    pool.timeBetweenEvictionRunsMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_TIME_BETWEEN_EVICTION_RUMS.toUpperCase()).toString());
                    pool.minEvictableIdleTimeMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_EVICTABLE_IDLE_TIME.toUpperCase()).toString());
                    loadPool(pool);
                }
            }
        }
    }

    private static void loadMainDS() {
        PoolDTO pool = new PoolDTO();
        pool.flag = null;
        pool.url = ConfigContainer.DB_JDBC_URL;
        pool.driver = ConfigContainer.DB_JDBC_DRIVER;
        pool.userName = ConfigContainer.DB_JDBC_USERNAME;
        pool.password = ConfigContainer.DB_JDBC_PASSWORD;
        pool.defaultAutoCommit = ConfigContainer.DB_POOL_DEFAULT_AUTO_COMMIT;
        pool.initialSize = ConfigContainer.DB_POOL_INITIAL_SIZE;
        pool.maxActive = ConfigContainer.DB_POOL_MAX_ACTIVE;
        pool.minIdle = ConfigContainer.DB_POOL_MIN_IDLE;
        pool.maxIdle = ConfigContainer.DB_POOL_MAX_IDLE;
        pool.maxWait = ConfigContainer.DB_POOL_MAX_WAIT;
        pool.removeAbandoned = ConfigContainer.DB_POOL_REMOVE_ABANDONED;
        pool.removeAbandonedTimeoutMillis = ConfigContainer.DB_POOL_REMOVE_ABANDONED_TIMEOUT;
        pool.timeBetweenEvictionRunsMillis = ConfigContainer.DB_POOL_TIME_BETWEEN_EVICTION_RUMS;
        pool.minEvictableIdleTimeMillis = ConfigContainer.DB_POOL_MIN_EVICTABLE_IDLE_TIME;
        loadPool(pool);
    }

    private static void loadPool(PoolDTO pool) {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(pool.url);
        ds.setDriverClassName(pool.driver);
        ds.setUsername(pool.userName);
        ds.setPassword(pool.password);
        ds.setDefaultAutoCommit(pool.defaultAutoCommit);
        ds.setInitialSize(pool.initialSize);
        ds.setMaxActive(pool.maxActive);
        ds.setMinIdle(pool.minIdle);
        ds.setMaxIdle(pool.maxIdle);
        ds.setMaxWait(pool.maxWait);
        ds.setRemoveAbandoned(pool.removeAbandoned);
        ds.setRemoveAbandonedTimeoutMillis(pool.removeAbandonedTimeoutMillis);
        ds.setTimeBetweenEvictionRunsMillis(pool.timeBetweenEvictionRunsMillis);
        ds.setMinEvictableIdleTimeMillis(pool.minEvictableIdleTimeMillis);
        MULTI_DS.put(pool.flag, ds);
        MULTI_DB_DIALECT.put(pool.flag, parseDialect(pool.url));
        MULTI_POOL.put(pool.flag, pool);
        logger.debug("Load pool: flag" + pool.flag + ",url:" + pool.url);
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
