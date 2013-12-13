package com.ecfront.easybi.dbutils.exchange;

import com.ecfront.easybi.dbutils.inner.DBExecutor;
import com.ecfront.easybi.dbutils.inner.DSLoader;
import com.ecfront.easybi.dbutils.inner.Transaction;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class DB<E> {

    public E getObject(String sql, Object[] params) {
        return DBExecutor.get(sql, params, entityClass, getConnection(dsCode));
    }

    public List<E> findObjects(String sql, Object[] params) {
        return DBExecutor.find(sql, params, entityClass, getConnection(dsCode));
    }

    public Page<E> findObjects(String sql, Object[] params, long pageNumber, long pageSize) {
        return DBExecutor.find(sql, params, pageNumber, pageSize, entityClass, getConnection(dsCode), getDialect(dsCode));
    }

    public Map<String, Object> get(String sql, Object[] params) {
        return DBExecutor.get(sql, params, getConnection(dsCode));
    }

    public List<Map<String, Object>> find(String sql, Object[] params) {
        return DBExecutor.find(sql, params, getConnection(dsCode));
    }

    public Page<Map<String, Object>> find(String sql, Object[] params, int pageNumber, int pageSize) {
        return DBExecutor.find(sql, params, pageNumber, pageSize, getConnection(dsCode), getDialect(dsCode));
    }

    public long count(String sql) {
        return DBExecutor.count(sql, getConnection(dsCode));
    }

    public long count(String sql, Object[] params) {
        return DBExecutor.count(sql, params, getConnection(dsCode));
    }

    public void update(String sql, Object[] params) {
        DBExecutor.update(sql, params, getConnection(dsCode));
    }

    public void batch(String sql, Object[][] params) {
        DBExecutor.batch(sql, params, getConnection(dsCode));
    }

    public void open() {
        connection = Transaction.open(dsCode);
    }

    public void commit() {
        Transaction.commit();
    }

    public void rollback() {
        Transaction.rollback();
    }

    private Connection getConnection(String dsCode) {
        if (null == connection) {
            return DSLoader.getConnection(dsCode);
        }
        return connection;
    }

    private Dialect getDialect(String dsCode) {
        return DSLoader.getDialect(dsCode);
    }

    private String dsCode;
    protected Connection connection;
    private Class<E> entityClass;

    public DB() {
        dsCode = null;
    }

    public DB(String dsCode) {
        this.dsCode = dsCode;
    }

    private void init() {
        if (this.getClass().getGenericSuperclass() instanceof ParameterizedType) {
            Type[] type = ((ParameterizedType) this.getClass()
                    .getGenericSuperclass()).getActualTypeArguments();
            entityClass = (Class<E>) type[0];
        }
    }

    {
        init();
    }
}
