package com.ecfront.easybi.base.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;


public class PropertyHelper {
    private static Map<String, String> properties;

    public static String get(String name, String defaultVal) {
        String val = get(name);
        if (null == val || "".equals(val.trim())) {
            return defaultVal;
        }
        return val;
    }

    public static String get(String name) {
        if (null == properties) {
            try {
                loadProperties();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return properties.containsKey(name) ? properties.get(name) : null;
    }

    private static void loadProperties() throws IOException {
        properties = new HashMap<String, String>();
        Properties prop = new Properties();
        File[] files = new File(PropertyHelper.class.getResource("/").getPath()).listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File file, String s) {
                return s.endsWith("properties");
            }
        });
        if (null != files && files.length > 0) {
            String key;
            for (File file : files) {
                prop.load(new FileInputStream(file));
                for (Iterator it = prop.keySet().iterator(); it.hasNext(); ) {
                    key = (String) it.next();
                    properties.put(key, (String) prop.get(key));
                }
            }
        }
    }
}
