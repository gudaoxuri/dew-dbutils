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
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DSLoader {

    private static final Map<String, DataSource> MULTI_DS = new HashMap<String, DataSource>();
    private static final Map<String, Dialect> MULTI_DB_DIALECT = new HashMap<String, Dialect>();
    private static final Map<String, DSEntity> MULTI_DS_ENTITY = new HashMap<String, DSEntity>();

    public static void reload() {
        MULTI_DS.clear();
        MULTI_DB_DIALECT.clear();
        MULTI_DS_ENTITY.clear();
        loadMainDS();
        if (ConfigContainer.MULTI_DS_SUPPORT) {
            loadMultiDS();
        }
    }

    public static Connection getConnection(String dsCode) {
        Connection connection = null;
        DSEntity dsEntity = MULTI_DS_ENTITY.get(dsCode);
        try {
            if (dsEntity.poolSupport) {
                connection = MULTI_DS.get(dsCode).getConnection();
                if (connection.isClosed()) {
                    //Re-setting connection when connection was close.
                    synchronized (DSLoader.class) {
                        logger.warn("Connection info:" + connection.toString() + " was close.");
                        loadPool(dsEntity);
                        connection = MULTI_DS.get(dsCode).getConnection();
                    }
                }
            } else {
                Class.forName(dsEntity.driver).newInstance();
                connection = DriverManager.getConnection(dsEntity.url, dsEntity.userName, dsEntity.password);
                if(null== connection){
                    logger.error("Connection can't create.");
                }
            }
        } catch (Exception e) {
            logger.error("Connection get error.", e);
        }
        return connection;
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
                    DSEntity dsEntity = new DSEntity();
                    dsEntity.flag = res.get(ConfigContainer.FLAG_CODE.toUpperCase()).toString();
                    dsEntity.poolSupport = Boolean.valueOf(res.get(ConfigContainer.FLAG_POOL_SUPPORT.toUpperCase()).toString());
                    dsEntity.monitor = Boolean.valueOf(res.get(ConfigContainer.FLAG_MONITOR.toUpperCase()).toString());
                    dsEntity.url = res.get(ConfigContainer.FLAG_URL.toUpperCase()).toString();
                    dsEntity.driver = res.get(ConfigContainer.FLAG_DRIVER.toUpperCase()).toString();
                    dsEntity.userName = res.get(ConfigContainer.FLAG_USERNAME.toUpperCase()).toString();
                    dsEntity.password = res.get(ConfigContainer.FLAG_PASSWORD.toUpperCase()).toString();
                    dsEntity.defaultAutoCommit = Boolean.valueOf(res.get(ConfigContainer.FLAG_DEFAULT_AUTO_COMMIT.toUpperCase()).toString());
                    dsEntity.initialSize = Integer.valueOf(res.get(ConfigContainer.FLAG_INITIAL_SIZE.toUpperCase()).toString());
                    dsEntity.maxActive = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_ACTIVE.toUpperCase()).toString());
                    dsEntity.minIdle = Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_IDLE.toUpperCase()).toString());
                    dsEntity.maxIdle = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_IDLE.toUpperCase()).toString());
                    dsEntity.maxWait = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_WAIT.toUpperCase()).toString());
                    dsEntity.removeAbandoned = Boolean.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED.toUpperCase()).toString());
                    dsEntity.removeAbandonedTimeoutMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED_TIMEOUT.toUpperCase()).toString());
                    dsEntity.timeBetweenEvictionRunsMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_TIME_BETWEEN_EVICTION_RUMS.toUpperCase()).toString());
                    dsEntity.minEvictableIdleTimeMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_EVICTABLE_IDLE_TIME.toUpperCase()).toString());
                    MULTI_DS_ENTITY.put(dsEntity.flag, dsEntity);
                    loadPool(dsEntity);
                }
            }
        }
    }

    private static void loadMainDS() {
        DSEntity dsEntity = new DSEntity();
        dsEntity.flag = null;
        dsEntity.url = ConfigContainer.DB_JDBC_URL;
        dsEntity.poolSupport = ConfigContainer.DB_POOL_SUPPORT;
        dsEntity.monitor = ConfigContainer.DB_POOL_MONITOR;
        dsEntity.driver = ConfigContainer.DB_JDBC_DRIVER;
        dsEntity.userName = ConfigContainer.DB_JDBC_USERNAME;
        dsEntity.password = ConfigContainer.DB_JDBC_PASSWORD;
        dsEntity.defaultAutoCommit = ConfigContainer.DB_POOL_DEFAULT_AUTO_COMMIT;
        dsEntity.initialSize = ConfigContainer.DB_POOL_INITIAL_SIZE;
        dsEntity.maxActive = ConfigContainer.DB_POOL_MAX_ACTIVE;
        dsEntity.minIdle = ConfigContainer.DB_POOL_MIN_IDLE;
        dsEntity.maxIdle = ConfigContainer.DB_POOL_MAX_IDLE;
        dsEntity.maxWait = ConfigContainer.DB_POOL_MAX_WAIT;
        dsEntity.removeAbandoned = ConfigContainer.DB_POOL_REMOVE_ABANDONED;
        dsEntity.removeAbandonedTimeoutMillis = ConfigContainer.DB_POOL_REMOVE_ABANDONED_TIMEOUT;
        dsEntity.timeBetweenEvictionRunsMillis = ConfigContainer.DB_POOL_TIME_BETWEEN_EVICTION_RUMS;
        dsEntity.minEvictableIdleTimeMillis = ConfigContainer.DB_POOL_MIN_EVICTABLE_IDLE_TIME;
        MULTI_DS_ENTITY.put(dsEntity.flag, dsEntity);
        loadPool(dsEntity);
    }

    private static void loadPool(DSEntity dsEntity) {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(dsEntity.url);
        ds.setDriverClassName(dsEntity.driver);
        ds.setUsername(dsEntity.userName);
        ds.setPassword(dsEntity.password);
        ds.setDefaultAutoCommit(dsEntity.defaultAutoCommit);
        ds.setInitialSize(dsEntity.initialSize);
        ds.setMaxActive(dsEntity.maxActive);
        ds.setMinIdle(dsEntity.minIdle);
        ds.setMaxIdle(dsEntity.maxIdle);
        ds.setMaxWait(dsEntity.maxWait);
        ds.setRemoveAbandoned(dsEntity.removeAbandoned);
        ds.setRemoveAbandonedTimeoutMillis(dsEntity.removeAbandonedTimeoutMillis);
        ds.setTimeBetweenEvictionRunsMillis(dsEntity.timeBetweenEvictionRunsMillis);
        ds.setMinEvictableIdleTimeMillis(dsEntity.minEvictableIdleTimeMillis);
        if (dsEntity.monitor) {
            try {
                ds.setFilters("wall,mergeStat");
            } catch (SQLException e) {
                logger.warn("Monitor set error.", e);
            }
        }
        MULTI_DS.put(dsEntity.flag, ds);
        MULTI_DB_DIALECT.put(dsEntity.flag, parseDialect(dsEntity.url));
        logger.debug("Load pool: flag:" + dsEntity.flag + ",url:" + dsEntity.url);
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
