package com.ecfront.easybi.dbutils.inner;


import com.ecfront.easybi.base.utils.PropertyHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigContainer {

    private static final Logger logger = LoggerFactory.getLogger(ConfigContainer.class);

    public static final String FLAG_CODE= "code";
    public static final String FLAG_POOL_SUPPORT= "poolSupport";
    public static final String FLAG_MONITOR= "monitor";
    public static final String FLAG_DRIVER = "driver";
    public static final String FLAG_URL = "url";
    public static final String FLAG_USERNAME = "username";
    public static final String FLAG_PASSWORD = "password";
    public static final String FLAG_INITIAL_SIZE = "initialSize";
    public static final String FLAG_MAX_ACTIVE = "maxActive";
    public static final String FLAG_MIN_IDLE = "minIdle";
    public static final String FLAG_MAX_IDLE = "maxIdle";
    public static final String FLAG_MAX_WAIT = "maxWait";
    public static final String FLAG_DEFAULT_AUTO_COMMIT="autoCommit";
    public static final String FLAG_REMOVE_ABANDONED="rmAbandoned";
    public static final String FLAG_REMOVE_ABANDONED_TIMEOUT="rmAbandonedTimeout";
    public static final String FLAG_TIME_BETWEEN_EVICTION_RUMS="betweenEvictionRuns";
    public static final String FLAG_MIN_EVICTABLE_IDLE_TIME="minEvictableIdle";


    public static Boolean MULTI_DS_SUPPORT;
    public static String MULTI_DS_QUERY;
    public static String DB_JDBC_DRIVER;
    public static String DB_JDBC_URL;
    public static String DB_JDBC_USERNAME;
    public static String DB_JDBC_PASSWORD;
    public static boolean DB_POOL_SUPPORT;
    public static String DB_POOL_TYPE;
    public static boolean DB_POOL_MONITOR;
    public static int DB_POOL_INITIAL_SIZE;
    public static int DB_POOL_MAX_ACTIVE;
    public static int DB_POOL_MIN_IDLE;
    public static int DB_POOL_MAX_IDLE;
    public static int DB_POOL_MAX_WAIT;
    public static boolean DB_POOL_DEFAULT_AUTO_COMMIT;
    public static boolean DB_POOL_REMOVE_ABANDONED;
    public static int DB_POOL_REMOVE_ABANDONED_TIMEOUT;
    public static int DB_POOL_TIME_BETWEEN_EVICTION_RUMS;
    public static int DB_POOL_MIN_EVICTABLE_IDLE_TIME;

    static {
        try {
            MULTI_DS_SUPPORT = null != PropertyHelper.get("ez_multi_ds_support") ? Boolean.valueOf(PropertyHelper.get("ez_multi_ds_support")) : true;
            MULTI_DS_QUERY = PropertyHelper.get("ez_multi_ds_query");
            DB_JDBC_DRIVER = PropertyHelper.get("ez_db_jdbc_driver");
            DB_JDBC_URL = PropertyHelper.get("ez_db_jdbc_url");
            DB_JDBC_USERNAME = PropertyHelper.get("ez_db_jdbc_username");
            DB_JDBC_PASSWORD = PropertyHelper.get("ez_db_jdbc_password");
            DB_POOL_SUPPORT = null != PropertyHelper.get("ez_db_pool_support") ? Boolean.valueOf(PropertyHelper.get("ez_db_pool_support")) : true;
            DB_POOL_TYPE = null != PropertyHelper.get("ez_db_pool_type") ? PropertyHelper.get("ez_db_pool_type") : "druid";
            DB_POOL_MONITOR = null != PropertyHelper.get("ez_db_pool_monitor") ? Boolean.valueOf(PropertyHelper.get("ez_db_pool_monitor")) : false;
            DB_POOL_INITIAL_SIZE = null != PropertyHelper.get("ez_db_pool_initialSize") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_initialSize")) : 10;
            DB_POOL_MAX_ACTIVE = null != PropertyHelper.get("ez_db_pool_maxActive") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_maxActive")) : 50;
            DB_POOL_MIN_IDLE = null != PropertyHelper.get("ez_db_pool_minIdle") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_minIdle")) : 5;
            DB_POOL_MAX_IDLE = null != PropertyHelper.get("ez_db_pool_maxIdle") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_maxIdle")) : 20;
            DB_POOL_MAX_WAIT = null != PropertyHelper.get("ez_db_pool_maxWait") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_maxWait")) : 6000;
            DB_POOL_DEFAULT_AUTO_COMMIT = null != PropertyHelper.get("ez_db_pool_defaultAutoCommit") ? Boolean.valueOf(PropertyHelper.get("ez_db_pool_defaultAutoCommit")) : true;
            DB_POOL_REMOVE_ABANDONED = null != PropertyHelper.get("ez_db_pool_removeAbandoned") ? Boolean.valueOf(PropertyHelper.get("ez_db_pool_removeAbandoned")) : true;
            DB_POOL_REMOVE_ABANDONED_TIMEOUT = null != PropertyHelper.get("ez_db_pool_removeAbandonedTimeoutMillis") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_removeAbandonedTimeoutMillis")) : 180;
            DB_POOL_TIME_BETWEEN_EVICTION_RUMS = null != PropertyHelper.get("ez_db_pool_timeBetweenEvictionRunsMillis") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_timeBetweenEvictionRunsMillis")) : 3600000;
            DB_POOL_MIN_EVICTABLE_IDLE_TIME = null != PropertyHelper.get("ez_db_pool_minEvictableIdleTimeMillis") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_minEvictableIdleTimeMillis")) : 3600000;
        }catch (Exception e){
            logger.error("Init error.",e);
        }
    }

}
