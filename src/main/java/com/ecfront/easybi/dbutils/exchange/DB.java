package com.ecfront.easybi.dbutils.exchange;

import com.ecfront.easybi.dbutils.inner.ConnectionWrap;
import com.ecfront.easybi.dbutils.inner.DBExecutor;
import com.ecfront.easybi.dbutils.inner.DSLoader;
import com.ecfront.easybi.dbutils.inner.Transaction;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * <h1>数据操作类</h1>
 */
public class DB {

    /**
     * 创建表
     *
     * @param tableName    表名
     * @param tableDesc    表说明
     * @param fields       表字段（字段名-> 类型）
     * @param fieldsDesc   字段说明
     * @param indexFields  索引字段
     * @param uniqueFields 唯一值字段
     * @param pkField      主键字段
     * @throws SQLException
     */
    public void createTableIfNotExist(String tableName, String tableDesc, Map<String, String> fields, Map<String, String> fieldsDesc, List<String> indexFields, List<String> uniqueFields, String pkField) throws SQLException {
        tableName = tableName.toLowerCase();
        if (find(getDialect(dsCode).getTableInfo(tableName)).isEmpty()) {
            DBExecutor.ddl(getDialect(dsCode).createTableIfNotExist(tableName, tableDesc, fields, fieldsDesc, indexFields, uniqueFields, pkField), getConnection(dsCode), isCloseConnection());
        }
    }

    /**
     * DDL操作
     *
     * @param ddl DDL语句
     */
    public void ddl(String ddl) throws SQLException {
        DBExecutor.ddl(ddl, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 获取单条记录
     *
     * @param tableName 表名
     * @param pkValue   主键值
     * @param clazz     对象类
     * @return java对象
     */
    public <E> E getObjectByPk(String tableName, Object pkValue, Class<E> clazz) throws SQLException {
        return getObject("SELECT * FROM " + tableName + " WHERE id= ?", new Object[]{pkValue}, clazz);
    }

    /**
     * 获取单个对象
     *
     * @param sql   SQL
     * @param clazz 对象类
     * @return java对象
     */
    public <E> E getObject(String sql, Class<E> clazz) throws SQLException {
        return getObject(sql, null, clazz);
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
        return findObjects(sql, null, clazz);
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
        return findObjects(sql, null, pageNumber, pageSize, clazz);
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
     * 判断记录是否存在
     *
     * @param tableName 表名
     * @param pkValue   主键值
     * @return 是否存在
     */
    public boolean containByKey(String tableName, Object pkValue) throws SQLException, IOException {
        return get("SELECT id FROM " + tableName + " WHERE id= ?", new Object[]{pkValue}).size() != 0;
    }

    /**
     * 判断记录是否存在
     *
     * @param sql    SQL
     * @param params 参数
     * @return 是否存在
     */
    public boolean contain(String sql, Object[] params) throws SQLException, IOException {
        return find(sql, params).size() != 0;
    }

    /**
     * 获取单条记录
     *
     * @param tableName 表名
     * @param pkValue   主键值
     * @return 单条记录
     */
    public Map<String, Object> getByPk(String tableName, Object pkValue) throws SQLException, IOException {
        return get("SELECT * FROM " + tableName + " WHERE id= ?", new Object[]{pkValue});
    }

    /**
     * 获取单条记录
     *
     * @param sql SQL
     * @return 单条记录
     */
    public Map<String, Object> get(String sql) throws SQLException, IOException {
        return get(sql, null);
    }

    /**
     * 获取单条记录
     *
     * @param sql    SQL
     * @param params 参数
     * @return 单条记录
     */
    public Map<String, Object> get(String sql, Object[] params) throws SQLException, IOException {
        return DBExecutor.get(sql, params, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 删除单条记录
     *
     * @param tableName 表名
     * @param pkValue   主键值
     * @return 单条记录
     */
    public Integer deleteByPk(String tableName, Object pkValue) throws SQLException {
        return update("DELETE FROM " + tableName + " WHERE id= ?", new Object[]{pkValue});
    }

    /**
     * 删除所有记录
     *
     * @param tableName 表名
     * @return 单条记录
     */
    public Integer deleteAll(String tableName) throws SQLException {
        return update("DELETE FROM " + tableName, null);
    }

    /**
     * 获取多条记录（带分页）
     *
     * @param sql SQL
     * @return 多条记录（带分页）
     */
    public List<Map<String, Object>> find(String sql) throws SQLException {
        return find(sql, null);
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
        return find(sql, null, pageNumber, pageSize);
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
        return count(sql, null);
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
     * 保存记录
     *
     * @param tableName 表名
     * @param values    值列表
     */
    public int save(String tableName, Map<String, Object> values) throws SQLException {
        return DBExecutor.updateModel(tableName, values, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 保存记录
     *
     * @param tableName 表名
     * @param pkValue   主键值
     * @param values    值列表
     */
    public int update(String tableName, Object pkValue, Map<String, Object> values) throws SQLException {
        return DBExecutor.updateModel(tableName, pkValue, values, getConnection(dsCode), isCloseConnection());
    }

    /**
     * 更新记录
     *
     * @param sql SQL
     */
    public int update(String sql) throws SQLException {
        return update(sql, null);
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

    /**
     * 批量更新记录
     *
     * @param sqls SQL
     */
    public void batch(Map<String, Object[]> sqls) throws SQLException {
        DBExecutor.batch(sqls, getConnection(dsCode), isCloseConnection());
    }

    public List<Meta> getMetaData(String tableName) throws SQLException {
        return DBExecutor.getMetaData(tableName, getConnection(dsCode));
    }

    public Meta getMetaData(String tableName, String fieldName) throws SQLException {
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

    public static String convertClob(Clob clob) throws SQLException, IOException {
        StringBuffer value = new StringBuffer();
        String line;
        if (clob != null) {
            Reader reader = clob.getCharacterStream();
            BufferedReader br = new BufferedReader(reader);
            while ((line = br.readLine()) != null) {
                value.append(line + "\r\n");
            }
        }
        if (value.length() >= 2) {
            return value.substring(0, value.length() - 2);
        } else {
            return "";
        }
    }

    private ConnectionWrap getConnection(String dsCode) {
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
    protected ConnectionWrap transactionConnection;

    public DB() {
        dsCode = null;
    }

    public DB(String dsCode) {
        this.dsCode = dsCode;
    }

    private static final Logger logger = LoggerFactory.getLogger(DB.class);


}
