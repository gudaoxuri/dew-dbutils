package com.ecfront.easybi.dbutils.exchange;

import com.ecfront.easybi.dbutils.inner.DBExecutor;
import com.ecfront.easybi.dbutils.inner.DSLoader;
import com.ecfront.easybi.dbutils.inner.Transaction;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public class DB {

    public <E> E getObject(String sql, Class<E> clazz) {
        return DBExecutor.get(sql, null, clazz, getConnection(dsCode));
    }

    public <E> E getObject(String sql, Object[] params, Class<E> clazz) {
        return DBExecutor.get(sql, params, clazz, getConnection(dsCode));
    }

    public <E> List<E> findObjects(String sql, Class<E> clazz) {
        return DBExecutor.find(sql, null, clazz, getConnection(dsCode));
    }

    public <E> List<E> findObjects(String sql, Object[] params, Class<E> clazz) {
        return DBExecutor.find(sql, params, clazz, getConnection(dsCode));
    }

    public <E> Page<E> findObjects(String sql, long pageNumber, long pageSize, Class<E> clazz) {
        return DBExecutor.find(sql, null, pageNumber, pageSize, clazz, getConnection(dsCode), getDialect(dsCode));
    }

    public <E> Page<E> findObjects(String sql, Object[] params, long pageNumber, long pageSize, Class<E> clazz) {
        return DBExecutor.find(sql, params, pageNumber, pageSize, clazz, getConnection(dsCode), getDialect(dsCode));
    }

    public Map<String, Object> get(String sql) {
        return DBExecutor.get(sql, null, getConnection(dsCode));
    }

    public Map<String, Object> get(String sql, Object[] params) {
        return DBExecutor.get(sql, params, getConnection(dsCode));
    }

    public List<Map<String, Object>> find(String sql) {
        return DBExecutor.find(sql, null, getConnection(dsCode));
    }

    public List<Map<String, Object>> find(String sql, Object[] params) {
        return DBExecutor.find(sql, params, getConnection(dsCode));
    }

    public Page<Map<String, Object>> find(String sql, int pageNumber, int pageSize) {
        return DBExecutor.find(sql, null, pageNumber, pageSize, getConnection(dsCode), getDialect(dsCode));
    }

    public Page<Map<String, Object>> find(String sql, Object[] params, int pageNumber, int pageSize) {
        return DBExecutor.find(sql, params, pageNumber, pageSize, getConnection(dsCode), getDialect(dsCode));
    }

    public long count(String sql) {
        return DBExecutor.count(sql, getConnection(dsCode), getDialect(dsCode));
    }

    public long count(String sql, Object[] params) {
        return DBExecutor.count(sql, params, getConnection(dsCode), getDialect(dsCode));
    }

    public void update(String sql) {
        DBExecutor.update(sql, null, getConnection(dsCode));
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
        connection=null;
    }

    public void rollback() {
        Transaction.rollback();
        connection=null;
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

    public DB() {
        dsCode = null;
    }

    public DB(String dsCode) {
        this.dsCode = dsCode;
    }

}
