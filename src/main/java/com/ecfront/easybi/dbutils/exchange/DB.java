package com.ecfront.easybi.dbutils.exchange;

import com.ecfront.easybi.dbutils.inner.DBExecutor;
import com.ecfront.easybi.dbutils.inner.DSLoader;
import com.ecfront.easybi.dbutils.inner.Transaction;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * <h1>数据操作类</h1>
 */
public class DB {

    /**
     * 获取单个对象
     *
     * @param sql   SQL
     * @param clazz 对象类
     * @return java对象
     */
    public <E> E getObject(String sql, Class<E> clazz) throws SQLException {
        return DBExecutor.get(sql, null, clazz, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取单个对象
     *
     * @param sql    SQL
     * @param params 参数
     * @param clazz  对象类
     * @return java对象
     */
    public <E> E getObject(String sql, Object[] params, Class<E> clazz) throws SQLException {
        return DBExecutor.get(sql, params, clazz, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取多个对象
     *
     * @param sql   SQL
     * @param clazz 对象类
     * @return java对象
     */
    public <E> List<E> findObjects(String sql, Class<E> clazz) throws SQLException {
        return DBExecutor.find(sql, null, clazz, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取多个对象
     *
     * @param sql    SQL
     * @param params 参数
     * @param clazz  对象类
     * @return java对象
     */
    public <E> List<E> findObjects(String sql, Object[] params, Class<E> clazz) throws SQLException {
        return DBExecutor.find(sql, params, clazz, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取多个对象（带分页）
     *
     * @param sql        SQL
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @param clazz      对象类
     * @return 多个对象（带分页）
     */
    public <E> Page<E> findObjects(String sql, long pageNumber, long pageSize, Class<E> clazz) throws SQLException {
        return DBExecutor.find(sql, null, pageNumber, pageSize, clazz, getConnection(dsCode), isCloseConnection(), getDialect(dsCode));
    }

    /**
     * 获取多个对象（带分页）
     *
     * @param sql        SQL
     * @param params     参数
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @param clazz      对象类
     * @return 多个对象（带分页）
     */
    public <E> Page<E> findObjects(String sql, Object[] params, long pageNumber, long pageSize, Class<E> clazz) throws SQLException {
        return DBExecutor.find(sql, params, pageNumber, pageSize, clazz, getConnection(dsCode), isCloseConnection(), getDialect(dsCode));
    }

    /**
     * 获取单条记录
     *
     * @param sql SQL
     * @return 单条记录
     */
    public Map<String, Object> get(String sql) throws SQLException {
        return DBExecutor.get(sql, null, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取单条记录
     *
     * @param sql    SQL
     * @param params 参数
     * @return 单条记录
     */
    public Map<String, Object> get(String sql, Object[] params) throws SQLException {
        return DBExecutor.get(sql, params, getConnection(dsCode), isCloseConnection());
    }


    /**
     * 获取多条记录（带分页）
     *
     * @param sql SQL
     * @return 多条记录（带分页）
     */
    public List<Map<String, Object>> find(String sql) throws SQLException {
        return DBExecutor.find(sql, null, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取多条记录（带分页）
     *
     * @param sql    SQL
     * @param params 参数
     * @return 多条记录（带分页）
     */
    public List<Map<String, Object>> find(String sql, Object[] params) throws SQLException {
        return DBExecutor.find(sql, params, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取多条记录（带分页）
     *
     * @param sql        SQL
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @return 多条记录（带分页）
     */
    public Page<Map<String, Object>> find(String sql, int pageNumber, int pageSize) throws SQLException {
        return DBExecutor.find(sql, null, pageNumber, pageSize, getConnection(dsCode), isCloseConnection(), getDialect(dsCode));
    }


    /**
     * 获取多条记录（带分页）
     *
     * @param sql        SQL
     * @param params     参数
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @return 多条记录（带分页）
     */
    public Page<Map<String, Object>> find(String sql, Object[] params, int pageNumber, int pageSize) throws SQLException {
        return DBExecutor.find(sql, params, pageNumber, pageSize, getConnection(dsCode), isCloseConnection(), getDialect(dsCode));
    }

    /**
     * 获取记录数
     *
     * @param sql SQL
     * @return 记录数
     */
    public long count(String sql) throws SQLException {
        return DBExecutor.count(sql, getConnection(dsCode), isCloseConnection(), getDialect(dsCode));
    }

    /**
     * 获取记录数
     *
     * @param sql    SQL
     * @param params 参数
     * @return 记录数
     */
    public long count(String sql, Object[] params) throws SQLException {
        return DBExecutor.count(sql, params, getConnection(dsCode), isCloseConnection(), getDialect(dsCode));
    }

    /**
     * 更新记录
     *
     * @param sql SQL
     */
    public int update(String sql) throws SQLException {
        return DBExecutor.update(sql, null, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 更新记录
     *
     * @param sql    SQL
     * @param params 参数
     */
    public int update(String sql, Object[] params) throws SQLException {
        return DBExecutor.update(sql, params, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 批量更新记录
     *
     * @param sql    SQL
     * @param params 参数
     */
    public int[] batch(String sql, Object[][] params) throws SQLException {
        return DBExecutor.batch(sql, params, getConnection(dsCode), isCloseConnection());
    }

    public List<Meta> getMetaData(String tableName) throws SQLException {
        return DBExecutor.getMetaData(tableName,getConnection(dsCode));
    }

    public Meta getMetaData(String tableName,String fieldName) throws SQLException {
        return DBExecutor.getMetaData(tableName, fieldName, getConnection(dsCode));
    }

    /**
     * 打开事务
     */
    public void open() {
        transactionConnection = Transaction.open(dsCode);
    }

    /**
     * 提交事务
     */
    public void commit() {
        Transaction.commit();
        transactionConnection = null;
    }

    /**
     * 显示回滚事务（发生SQL错误时会自动回滚，但业务错误需要调用此方法手工回滚）
     */
    public void rollback() {
        Transaction.rollback();
        transactionConnection = null;
    }

    private Connection getConnection(String dsCode) {
        if (null == transactionConnection) {
            return DSLoader.getConnection(dsCode);
        }
        return transactionConnection;
    }

    private boolean isCloseConnection() {
        return null == transactionConnection;
    }

    private Dialect getDialect(String dsCode) {
        return DSLoader.getDialect(dsCode);
    }

    private String dsCode;
    protected Connection transactionConnection;

    public DB() {
        dsCode = null;
    }

    public DB(String dsCode) {
        this.dsCode = dsCode;
    }

    private static final Logger logger = LoggerFactory.getLogger(DB.class);


}
