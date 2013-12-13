package com.ecfront.easybi.dbutils.exchange;

import java.util.List;

public class Page<E> {
    //start 1
    public long pageNumber;
    public long pageSize;
    public long pageTotal;
    public long recordTotal;
    public List<E> objects;

}
