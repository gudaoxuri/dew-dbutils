package com.ecfront.easybi.dbutils.exchange;

import com.ecfront.easybi.dbutils.inner.DSLoader;

/**
 * <h1>数据源操作</h1>
 */
public class DS {

    /**
     * 重载数据源，当数据源配置变更时需要调用此方法。
     */
    public static void reload() {
        DSLoader.reload();
    }

}
