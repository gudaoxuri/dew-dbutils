package com.ecfront.easybi.dbutils.exchange;

import java.util.List;

/**
 * 分页辅助类
 */
public class Page<E> {
    //start with 1
    public long pageNumber;
    public long pageSize;
    public long pageTotal;
    public long recordTotal;
    public List<E> objects;

}
