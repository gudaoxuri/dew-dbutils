package com.ecfront.easybi.dbutils.inner;


import com.ecfront.easybi.base.utils.PropertyHelper;

public class ConfigContainer {

    public static final String FLAG_CODE= "code";
    public static final String FLAG_DRIVER = "driver";
    public static final String FLAG_URL = "url";
    public static final String FLAG_USERNAME = "username";
    public static final String FLAG_PASSWORD = "password";
    public static final String FLAG_INITIAL_SIZE = "initialSize";
    public static final String FLAG_MAX_ACTIVE = "maxActive";
    public static final String FLAG_MIN_IDLE = "minIdle";
    public static final String FLAG_MAX_IDLE = "maxIdle";
    public static final String FLAG_MAX_WAIT = "maxWait";


    public static Boolean IS_MULTI_DS_SUPPORT;
    public static String MULTI_DS_QUERY;
    public static String DB_JDBC_DRIVER;
    public static String DB_JDBC_URL;
    public static String DB_JDBC_USERNAME;
    public static String DB_JDBC_PASSWORD;
    public static int DB_POOL_INITIAL_SIZE;
    public static int DB_POOL_MAX_ACTIVE;
    public static int DB_POOL_MIN_IDLE;
    public static int DB_POOL_MAX_IDLE;
    public static int DB_POOL_MAX_WAIT;

    static {
        String temp = PropertyHelper.get("ez_multi_ds_support");
        IS_MULTI_DS_SUPPORT = null != temp && "true".equalsIgnoreCase(temp.trim()) ? true : false;
        MULTI_DS_QUERY = PropertyHelper.get("ez_multi_ds_query");
        DB_JDBC_DRIVER = PropertyHelper.get("ez_db_jdbc_driver");
        DB_JDBC_URL = PropertyHelper.get("ez_db_jdbc_url");
        DB_JDBC_USERNAME = PropertyHelper.get("ez_db_jdbc_username");
        DB_JDBC_PASSWORD = PropertyHelper.get("ez_db_jdbc_password");
        DB_POOL_INITIAL_SIZE = null != PropertyHelper.get("ez_db_pool_initialSize") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_initialSize")) : 10;
        DB_POOL_MAX_ACTIVE = null != PropertyHelper.get("ez_db_pool_maxActive") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_maxActive")) : 50;
        DB_POOL_MIN_IDLE = null != PropertyHelper.get("ez_db_pool_minIdle") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_minIdle")) : 5;
        DB_POOL_MAX_IDLE = null != PropertyHelper.get("ez_db_pool_maxIdle") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_maxIdle")) : 20;
        DB_POOL_MAX_WAIT = null != PropertyHelper.get("ez_db_pool_maxWait") ? Integer.valueOf(PropertyHelper.get("ez_db_pool_maxWait")) : 6000;
    }

}
