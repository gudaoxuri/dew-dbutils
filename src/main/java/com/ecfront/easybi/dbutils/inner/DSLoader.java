package com.ecfront.easybi.dbutils.inner;

import com.alibaba.druid.pool.DruidDataSource;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import com.ecfront.easybi.dbutils.inner.dialect.DialectFactory;
import org.apache.commons.dbcp.BasicDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
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

    public static ConnectionWrap getConnection(String dsCode) {
        ConnectionWrap cw = new ConnectionWrap();
        DSEntity dsEntity = MULTI_DS_ENTITY.get(dsCode);
        cw.type = DialectFactory.getDialectType(dsEntity.driver);
        try {
            if (dsEntity.poolSupport) {
                cw.conn = MULTI_DS.get(dsCode).getConnection();
                if (cw.conn.isClosed()) {
                    //Re-setting connection when connection was close.
                    synchronized (DSLoader.class) {
                        logger.warn("Connection info:" + cw.conn.toString() + " was close.");
                        loadPool(dsEntity);
                        cw.conn = MULTI_DS.get(dsCode).getConnection();
                    }
                }
            } else {
                Class.forName(dsEntity.driver).newInstance();
                cw.conn = DriverManager.getConnection(dsEntity.url, dsEntity.userName, dsEntity.password);
                if (null == cw.conn) {
                    logger.error("Connection can't create.");
                }
            }
        } catch (Exception e) {
            logger.error("Connection get error.", e);
        }
        return cw;
    }

    public static Dialect getDialect(String dsCode) {
        return MULTI_DB_DIALECT.get(dsCode);
    }

    private static void loadMultiDS() {
        List<Map<String, Object>> result = null;
        if (ConfigContainer.MULTI_DS_QUERY != null && !"".equals(ConfigContainer.MULTI_DS_QUERY.trim())) {
            try {
                result = DBExecutor.find(ConfigContainer.MULTI_DS_QUERY, null, getConnection(null), true);
            } catch (Exception e) {
                logger.error("Multi DS load error : " + e);
            }
        }
        if (null != result) {
            for (Map<String, Object> res : result) {
                if (null != res) {
                    DSEntity dsEntity = new DSEntity();
                    dsEntity.flag = res.get(ConfigContainer.FLAG_CODE.toUpperCase()).toString();
                    if (res.get(ConfigContainer.FLAG_POOL_SUPPORT.toUpperCase()) != null) {
                        dsEntity.poolSupport = Boolean.valueOf(res.get(ConfigContainer.FLAG_POOL_SUPPORT.toUpperCase()).toString());
                    } else {
                        dsEntity.poolSupport = true;
                    }
                    if (res.get(ConfigContainer.FLAG_MONITOR.toUpperCase()) != null) {
                        dsEntity.monitor = Boolean.valueOf(res.get(ConfigContainer.FLAG_MONITOR.toUpperCase()).toString());
                    } else {
                        dsEntity.monitor = true;
                    }
                    dsEntity.url = res.get(ConfigContainer.FLAG_URL.toUpperCase()).toString();
                    dsEntity.driver = res.get(ConfigContainer.FLAG_DRIVER.toUpperCase()).toString();
                    if (res.get(ConfigContainer.FLAG_USERNAME.toUpperCase()) != null) {
                        dsEntity.userName = res.get(ConfigContainer.FLAG_USERNAME.toUpperCase()).toString();
                    }
                    if (res.get(ConfigContainer.FLAG_PASSWORD.toUpperCase()) != null) {
                        dsEntity.password = res.get(ConfigContainer.FLAG_PASSWORD.toUpperCase()).toString();
                    }
                    if (res.get(ConfigContainer.FLAG_DEFAULT_AUTO_COMMIT.toUpperCase()) != null) {
                        dsEntity.defaultAutoCommit = Boolean.valueOf(res.get(ConfigContainer.FLAG_DEFAULT_AUTO_COMMIT.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_INITIAL_SIZE.toUpperCase()) != null) {
                        dsEntity.initialSize = Integer.valueOf(res.get(ConfigContainer.FLAG_INITIAL_SIZE.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_MAX_ACTIVE.toUpperCase()) != null) {
                        dsEntity.maxActive = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_ACTIVE.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_MIN_IDLE.toUpperCase()) != null) {
                        dsEntity.minIdle = Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_IDLE.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_MAX_IDLE.toUpperCase()) != null) {
                        dsEntity.maxIdle = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_IDLE.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_MAX_WAIT.toUpperCase()) != null) {
                        dsEntity.maxWait = Integer.valueOf(res.get(ConfigContainer.FLAG_MAX_WAIT.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_REMOVE_ABANDONED.toUpperCase()) != null) {
                        dsEntity.removeAbandoned = Boolean.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_REMOVE_ABANDONED_TIMEOUT.toUpperCase()) != null) {
                        dsEntity.removeAbandonedTimeoutMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_REMOVE_ABANDONED_TIMEOUT.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_TIME_BETWEEN_EVICTION_RUMS.toUpperCase()) != null) {
                        dsEntity.timeBetweenEvictionRunsMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_TIME_BETWEEN_EVICTION_RUMS.toUpperCase()).toString());
                    }
                    if (res.get(ConfigContainer.FLAG_MIN_EVICTABLE_IDLE_TIME.toUpperCase()) != null) {
                        dsEntity.minEvictableIdleTimeMillis = Integer.valueOf(res.get(ConfigContainer.FLAG_MIN_EVICTABLE_IDLE_TIME.toUpperCase()).toString());
                    }
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
        if (ConfigContainer.DB_POOL_TYPE.equalsIgnoreCase("druid")) {
            MULTI_DS.put(dsEntity.flag, loadDruidPool(dsEntity));
        } else if (ConfigContainer.DB_POOL_TYPE.equalsIgnoreCase("dbcp")) {
            MULTI_DS.put(dsEntity.flag, loadDBCPPool(dsEntity));
        }
        MULTI_DB_DIALECT.put(dsEntity.flag, DialectFactory.parseDialect(dsEntity.driver));
        logger.debug("Load pool: flag:" + dsEntity.flag + ",url:" + dsEntity.url);
    }

    private static DataSource loadDruidPool(DSEntity dsEntity) {
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
        return ds;
    }

    private static DataSource loadDBCPPool(DSEntity dsEntity) {
        BasicDataSource ds = new BasicDataSource();
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
        ds.setTimeBetweenEvictionRunsMillis(dsEntity.timeBetweenEvictionRunsMillis);
        ds.setMinEvictableIdleTimeMillis(dsEntity.minEvictableIdleTimeMillis);
        return ds;
    }

    private static final Logger logger = LoggerFactory.getLogger(DSLoader.class);

    static {
        reload();
    }


}
