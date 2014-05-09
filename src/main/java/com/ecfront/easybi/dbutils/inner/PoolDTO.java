package com.ecfront.easybi.dbutils.inner;

/**
 * Created by 震宇 on 2014/5/9.
 */
public class PoolDTO {

    public String flag;
    public String url;
    public String driver;
    public String userName;
    public String password;
    public boolean defaultAutoCommit;
    public int initialSize;
    public int maxActive;
    public int minIdle;
    public int maxIdle;
    public int maxWait;
    public boolean removeAbandoned;
    public int removeAbandonedTimeoutMillis;
    public int timeBetweenEvictionRunsMillis;
    public int minEvictableIdleTimeMillis;

}
