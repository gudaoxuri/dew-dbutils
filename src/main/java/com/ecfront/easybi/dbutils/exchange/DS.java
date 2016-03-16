package com.ecfront.easybi.dbutils.exchange;

import com.ecfront.easybi.dbutils.inner.DSLoader;

/**
 * <h1>数据源操作</h1>
 */
public class DS {

    /**
     * 重载数据源，当数据源配置变更时需要调用此方法
     */
    public static void reload() {
        DSLoader.reload();
    }

    /**
     * 添加数据源
     *
     * @param dsEntity 数据源
     */
    public static void add(DSEntity dsEntity) {
        DSLoader.addMultiDS(dsEntity);
    }

    /**
     * 添加数据源
     *
     * @param flag     数据源标记
     * @param url      jdbc url
     * @param userName 用户名
     * @param password 密码
     */
    public static void add(String flag, String url, String userName, String password) {
        DSLoader.addMultiDS(flag, url, userName, password);
    }

}
