package com.ecfront.easybi.dbutils.inner;

import com.alibaba.druid.pool.DruidDataSource;
import com.ecfront.easybi.dbutils.exchange.DSEntity;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import com.ecfront.easybi.dbutils.inner.dialect.DialectFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DSLoader {

    private static final Map<String, DataSource> MULTI_DS = new HashMap<>();
    private static final Map<String, Dialect> MULTI_DB_DIALECT = new HashMap<>();
    private static final Map<String, DSEntity> MULTI_DS_ENTITY = new HashMap<>();

    public static void reload() {
        MULTI_DS.clear();
        MULTI_DB_DIALECT.clear();
        MULTI_DS_ENTITY.clear();
        if (!ConfigContainer.MULTI_DS_SUPPORT) {
            loadMainDS();
        } else if (ConfigContainer.MULTI_DS_QUERY != null) {
            loadMainDS();
            loadMultiDS();
        } else {
            logger.info("Please Use API to add multi-ds.");
        }
    }

    public static void addMultiDS(DSEntity dsEntity) {
        MULTI_DS_ENTITY.put(dsEntity.flag, dsEntity);
        loadPool(dsEntity);
    }

    public static void addMultiDS(String flag, String url, String userName, String password) {
        DSEntity dsEntity = new DSEntity();
        dsEntity.flag = flag;
        dsEntity.url = url;
        dsEntity.poolSupport = true;
        dsEntity.monitor = false;
        dsEntity.userName = userName;
        dsEntity.password = password;
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

    public static ConnectionWrap getConnection(String dsCode) {
        ConnectionWrap cw = new ConnectionWrap();
        DSEntity dsEntity = MULTI_DS_ENTITY.get(dsCode);
        Dialect dialect = getDialect(dsCode);
        cw.type = dialect.getDialectType();
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
                Class.forName(dialect.getDriver()).newInstance();
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
            result.stream().filter(res -> null != res).forEach(res -> {
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
                addMultiDS(dsEntity);
            });
        }
    }

    private static void loadMainDS() {
        DSEntity dsEntity = new DSEntity();
        dsEntity.flag = null;
        dsEntity.url = ConfigContainer.DB_JDBC_URL;
        dsEntity.poolSupport = ConfigContainer.DB_POOL_SUPPORT;
        dsEntity.monitor = ConfigContainer.DB_POOL_MONITOR;
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
        MULTI_DS_ENTITY.put(null, dsEntity);
        loadPool(dsEntity);
    }

    private static void loadPool(DSEntity dsEntity) {
        Dialect dialect = DialectFactory.parseDialect(dsEntity.url);
        assert dialect != null;
        MULTI_DS.put(dsEntity.flag, loadDruidPool(dsEntity, dialect.getDriver()));
        MULTI_DB_DIALECT.put(dsEntity.flag, dialect);
        logger.debug("Load pool: flag:" + dsEntity.flag + ",url:" + dsEntity.url);
    }

    private static DataSource loadDruidPool(DSEntity dsEntity, String driver) {
        DruidDataSource ds = new DruidDataSource();
        ds.setUrl(dsEntity.url);
        ds.setDriverClassName(driver);
        ds.setUsername(dsEntity.userName);
        ds.setPassword(dsEntity.password);
        ds.setDefaultAutoCommit(dsEntity.defaultAutoCommit);
        if (dsEntity.initialSize != 0) {
            ds.setInitialSize(dsEntity.initialSize);
        }
        if (dsEntity.maxActive != 0) {
            ds.setMaxActive(dsEntity.maxActive);
        }
        if (dsEntity.minIdle != 0) {
            ds.setMinIdle(dsEntity.minIdle);
        }
        if (dsEntity.maxWait != 0) {
            ds.setMaxWait(dsEntity.maxWait);
        }
        ds.setRemoveAbandoned(dsEntity.removeAbandoned);
        if (dsEntity.removeAbandonedTimeoutMillis != 0) {
            ds.setRemoveAbandonedTimeoutMillis(dsEntity.removeAbandonedTimeoutMillis);
        }
        if (dsEntity.timeBetweenEvictionRunsMillis != 0) {
            ds.setTimeBetweenEvictionRunsMillis(dsEntity.timeBetweenEvictionRunsMillis);
        }
        if (dsEntity.minEvictableIdleTimeMillis != 0) {
            ds.setMinEvictableIdleTimeMillis(dsEntity.minEvictableIdleTimeMillis);
        }
        if (dsEntity.monitor) {
            try {
                ds.setFilters("wall,mergeStat");
            } catch (SQLException e) {
                logger.warn("Monitor set error.", e);
            }
        }
        return ds;
    }

    private static final Logger logger = LoggerFactory.getLogger(DSLoader.class);

    static {
        reload();
    }


}
