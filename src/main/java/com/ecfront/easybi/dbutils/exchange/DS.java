package com.ecfront.easybi.dbutils.exchange;

import com.ecfront.easybi.base.utils.PropertyHelper;
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

    /**
     * （可选）设置配置文件路径，默认在classpath下，此设置必须在DB被调用前。
     */
    public static void setConfigPath(String path) {
        PropertyHelper.setPropertiesPath(path);
    }

}
