package com.ecfront.easybi.dbutils.inner.dialect;

public class OracleDialect implements Dialect {

    @Override
    public String paging(String sql, long pageNumber, long pageSize) {
        return "select * from (select rownum rn , originaltable.* from ("+ sql + ") originaltable  where rownum<=" + (pageNumber * pageSize) + ") where rn > " + (pageNumber - 1) * pageSize;
    }

    @Override
    public String count(String sql) {
        return "select count(1) from ( "+sql+" ) ";
    }
}
