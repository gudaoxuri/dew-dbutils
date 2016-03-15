package com.ecfront.easybi.dbutils.inner;

import com.ecfront.easybi.dbutils.exchange.DB;
import com.ecfront.easybi.dbutils.exchange.Meta;
import com.ecfront.easybi.dbutils.exchange.Page;
import com.ecfront.easybi.dbutils.inner.dbutilsext.QueryRunnerExt;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import com.ecfront.easybi.dbutils.inner.dialect.DialectType;
import org.apache.commons.dbutils.handlers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DBExecutor {

    static final QueryRunnerExt queryRunner = new QueryRunnerExt();

    public static <E> E get(String sql, Object[] params, Class<E> clazz, ConnectionWrap cw, boolean isCloseConn) throws SQLException {
        E object = null;
        try {
            if (params == null) {
                object = (E) queryRunner.query(cw, sql, new BeanHandler(clazz));
            } else {
                object = (E) queryRunner.query(cw, sql, new BeanHandler(clazz), params);
            }
        } catch (SQLException e) {
            logger.error("Get error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
        return object;
    }

    public static <E> List<E> find(String sql, Object[] params, Class<E> clazz, ConnectionWrap cw, boolean isCloseConn) throws SQLException {
        List<E> list = null;
        try {
            if (null == params) {
                list = (List<E>) queryRunner.query(cw, sql, new BeanListHandler(clazz));
            } else {
                list = (List<E>) queryRunner.query(cw, sql, new BeanListHandler(clazz), params);
            }
        } catch (SQLException e) {
            logger.error("Find error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
        return list;
    }

    public static <E> Page<E> find(String sql, Object[] params, long pageNumber, long pageSize, Class<E> clazz, ConnectionWrap cw, boolean isCloseConn, Dialect dialect) throws SQLException {
        Page<E> page = new Page<>();
        String pagedSql = dialect.paging(sql, pageNumber, pageSize);
        page.pageNumber = pageNumber;
        page.pageSize = pageSize;
        page.recordTotal = count(sql, params, cw, false, dialect);
        page.pageTotal = (page.recordTotal + pageSize - 1) / pageSize;
        page.objects = find(pagedSql, params, clazz, cw, isCloseConn);
        return page;
    }

    public static Map<String, Object> get(String sql, Object[] params, ConnectionWrap cw, boolean isCloseConn) throws SQLException, IOException {
        Map<String, Object> map = null;
        try {
            if (null == params) {
                map = queryRunner.query(cw, sql, new MapHandler());
            } else {
                map = queryRunner.query(cw, sql, new MapHandler(), params);
            }
        } catch (SQLException e) {
            logger.error("Get error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
        if (map != null) {
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                if (entry.getValue() instanceof Clob) {
                    entry.setValue(DB.convertClob((Clob) entry.getValue()));
                }
            }
        }
        return map;
    }

    public static List<Map<String, Object>> find(String sql, Object[] params, ConnectionWrap cw, boolean isCloseConn) throws SQLException {
        List<Map<String, Object>> list = null;
        try {
            if (null == params) {
                list = queryRunner.query(cw, sql, new MapListHandler());
            } else {
                list = queryRunner.query(cw, sql, new MapListHandler(), params);
            }
        } catch (SQLException e) {
            logger.error("Find error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
        return list;
    }

    public static Page<Map<String, Object>> find(String sql, Object[] params, long pageNumber, long pageSize, ConnectionWrap cw, boolean isCloseConn, Dialect dialect) throws SQLException {
        Page<Map<String, Object>> page = new Page<>();
        String pagedSql = dialect.paging(sql, pageNumber, pageSize);
        page.pageNumber = pageNumber;
        page.pageSize = pageSize;
        page.recordTotal = count(sql, params, cw, false, dialect);
        page.pageTotal = (page.recordTotal + pageSize - 1) / pageSize;
        page.objects = find(pagedSql, params, cw, isCloseConn);
        return page;
    }

    public static long count(String sql, ConnectionWrap cw, boolean isCloseConn, Dialect dialect) throws SQLException {
        return count(sql, null, cw, isCloseConn, dialect);
    }

    public static long count(String sql, Object[] params, ConnectionWrap cw, boolean isCloseConn, Dialect dialect) throws SQLException {
        String countSql = dialect.count(sql);
        try {
            if (null == params) {
                return (Long) queryRunner.query(cw, countSql, scalarHandler);
            } else {
                return (Long) queryRunner.query(cw, countSql, scalarHandler, params);
            }

        } catch (SQLException e) {
            logger.error("Count error : " + countSql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
    }

    public static int updateModel(String tableName, Map<String, Object> values, ConnectionWrap connection, boolean closeConnection) throws SQLException {
        StringBuilder sb = new StringBuilder("INSERT INTO " + tableName + " ( ");
        StringBuilder keys = new StringBuilder();
        StringBuilder valueList = new StringBuilder();
        List params = new ArrayList<>();
        for (Map.Entry<String, Object> field : values.entrySet()) {
            keys.append(field.getKey()).append(",");
            valueList.append("?,");
            params.add(field.getValue());
        }
        sb.append(keys.substring(0, keys.length() - 1)).append(") VALUES ( ").append(valueList.substring(0, valueList.length() - 1)).append(" )");
        return update(sb.toString(), params.toArray(new Object[params.size()]), connection, closeConnection);
    }

    public static int updateModel(String tableName, Object pkValue, Map<String, Object> values, ConnectionWrap connection, boolean closeConnection) throws SQLException {
        StringBuilder sb = new StringBuilder("UPDATE " + tableName + " SET ");
        List params = new ArrayList<>();
        for (Map.Entry<String, Object> field : values.entrySet()) {
            sb.append(field.getKey() + "=? ,");
            params.add(field.getValue());
        }
        params.add(pkValue);
        return update(sb.substring(0, sb.length() - 1) + " WHERE id = ? ", params.toArray(new Object[params.size()]), connection, closeConnection);
    }

    public static int update(String sql, Object[] params, ConnectionWrap cw, boolean isCloseConn) throws SQLException {
        if (cw.type == DialectType.HIVE && params != null) {
            throw new SQLException("SparkSQL don't support [params] parameter.");
        }
        try {
            if (null == params) {
                return queryRunner.update(cw, sql);
            } else {
                return queryRunner.update(cw, sql, params);
            }
        } catch (SQLException e) {
            try {
                cw.conn.rollback();
            } catch (SQLException e1) {
                logger.error("Connection error : " + sql, e1);
                throw e1;
            }
            logger.error("Update error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
    }

    public static void batch(Map<String, Object[]> sqls, ConnectionWrap cw, boolean isCloseConn) throws SQLException {
        if (cw.type == DialectType.HIVE) {
            throw new SQLException("SparkSQL don't support [batch] method.");
        }
        for (Map.Entry<String, Object[]> entry : sqls.entrySet()) {
            try {
                if (null == entry.getValue()) {
                    queryRunner.update(cw, entry.getKey());
                } else {
                    queryRunner.update(cw, entry.getKey(), entry.getValue());
                }
            } catch (SQLException e) {
                try {
                    cw.conn.rollback();
                } catch (SQLException e1) {
                    logger.error("Connection error : " + entry.getKey(), e1);
                    throw e1;
                }
                logger.error("Batch error : " + entry.getKey(), e);
                throw e;
            } finally {
                if (isCloseConn) {
                    closeConnection(cw.conn);
                }
            }
        }
    }

    public static int[] batch(String sql, Object[][] params, ConnectionWrap cw, boolean isCloseConn) throws SQLException {
        if (cw.type == DialectType.HIVE) {
            throw new SQLException("SparkSQL don't support [batch] method.");
        }
        try {
            return queryRunner.batch(cw, sql, params);
        } catch (SQLException e) {
            try {
                cw.conn.rollback();
            } catch (SQLException e1) {
                logger.error("Connection error : " + sql, e1);
                throw e1;
            }
            logger.error("Batch error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
    }

    public static List<Meta> getMetaData(String tableName, ConnectionWrap cw) throws SQLException {
        return findMetaData(tableName, null, cw);
    }

    public static Meta getMetaData(String tableName, String fieldName, ConnectionWrap cw) throws SQLException {
        List<Meta> metas = findMetaData(tableName, fieldName, cw);
        if (null != metas && metas.size() == 1) {
            return metas.get(0);
        }
        return null;
    }

    private static List<Meta> findMetaData(String tableName, String fieldName, ConnectionWrap cw) throws SQLException {
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            st = cw.conn.prepareStatement("select * from " + tableName + " where 1=2");
            rs = st.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            List<Meta> metas = new ArrayList<>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                String columnName = meta.getColumnName(i).substring(meta.getColumnName(i).lastIndexOf(".") + 1);
                String columnLabel = meta.getColumnLabel(i).substring(meta.getColumnLabel(i).lastIndexOf(".") + 1);
                if (null != fieldName && !columnLabel.equalsIgnoreCase(fieldName)) {
                    continue;
                }
                metas.add(new Meta(meta.getColumnType(i), columnName.toLowerCase(), columnLabel.toLowerCase()));
            }
            return metas;
        } catch (SQLException e) {
            logger.error("getResultSet error : " + tableName, e);
            throw e;
        } finally {
            if (null != rs) {
                rs.close();
            }
            if (null != st) {
                st.close();
            }
            closeConnection(cw.conn);
        }
    }

    public static void ddl(String sql, ConnectionWrap cw, boolean isCloseConn) throws SQLException {
        try {
            logger.debug("Execute DDL : " + sql);
            queryRunner.update(cw, sql);
        } catch (SQLException e) {
            try {
                cw.conn.rollback();
            } catch (SQLException e1) {
                logger.error("Connection error : " + sql, e1);
                throw e1;
            }
            logger.error("ddl error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(cw.conn);
            }
        }
    }

    private static void closeConnection(Connection conn) throws SQLException {
        if (null != conn && !conn.isClosed()) {
            try {
                logger.debug("Close connection:" + conn.toString());
                conn.close();
            } catch (SQLException e) {
                logger.error("Close transactionConnection error : ", e);
                throw e;
            }
        }
    }

    private static ScalarHandler scalarHandler = new ScalarHandler() {
        @Override
        public Object handle(ResultSet rs) throws SQLException {
            Object obj = super.handle(rs);
            if (obj instanceof BigDecimal) {
                return ((BigDecimal) obj).longValue();
            } else if (obj instanceof Long) {
                return obj;
            } else {
                return ((Number) obj).longValue();
            }
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(DBExecutor.class);

}
