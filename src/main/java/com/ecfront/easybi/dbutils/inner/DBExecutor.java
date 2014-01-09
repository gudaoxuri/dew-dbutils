package com.ecfront.easybi.dbutils.inner;

import com.ecfront.easybi.dbutils.exchange.Meta;
import com.ecfront.easybi.dbutils.exchange.Page;
import com.ecfront.easybi.dbutils.inner.dialect.Dialect;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class DBExecutor {

    public static final QueryRunner queryRunner = new QueryRunner();

    public static <E> E get(String sql, Object[] params, Class<E> clazz, Connection conn, boolean isCloseConn) throws SQLException {
        E object = null;
        try {
            if (params == null) {
                object = (E) queryRunner.query(conn, sql, new BeanHandler(clazz));
            } else {
                object = (E) queryRunner.query(conn, sql, new BeanHandler(clazz), params);
            }
        } catch (SQLException e) {
            logger.error("Get error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(conn);
            }
        }
        return object;
    }

    public static <E> List<E> find(String sql, Object[] params, Class<E> clazz, Connection conn, boolean isCloseConn) throws SQLException {
        List<E> list = null;
        try {
            if (null == params) {
                list = (List<E>) queryRunner.query(conn, sql, new BeanListHandler(clazz));
            } else {
                list = (List<E>) queryRunner.query(conn, sql, new BeanListHandler(clazz), params);
            }
        } catch (SQLException e) {
            logger.error("Find error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(conn);
            }
        }
        return list;
    }

    public static <E> Page<E> find(String sql, Object[] params, long pageNumber, long pageSize, Class<E> clazz, Connection conn, boolean isCloseConn, Dialect dialect) throws SQLException {
        Page<E> page = new Page<E>();
        String pagedSql = dialect.paging(sql, pageNumber, pageSize);
        page.pageNumber = pageNumber;
        page.pageSize = pageSize;
        page.recordTotal = count(pagedSql, params, conn, false, dialect);
        page.pageTotal = (page.recordTotal + pageSize - 1) / pageSize;
        page.objects = find(pagedSql, params, clazz, conn, isCloseConn);
        return page;
    }

    public static Map<String, Object> get(String sql, Object[] params, Connection conn, boolean isCloseConn) throws SQLException {
        Map<String, Object> map = null;
        try {
            if (null == params) {
                map = queryRunner.query(conn, sql, new MapHandler());
            } else {
                map = queryRunner.query(conn, sql, new MapHandler(), params);
            }
        } catch (SQLException e) {
            logger.error("Get error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(conn);
            }
        }
        return map;
    }

    public static List<Map<String, Object>> find(String sql, Object[] params, Connection conn, boolean isCloseConn) throws SQLException {
        List<Map<String, Object>> list = null;
        try {
            if (null == params) {
                list = queryRunner.query(conn, sql, new MapListHandler());
            } else {
                list = queryRunner.query(conn, sql, new MapListHandler(), params);
            }
        } catch (SQLException e) {
            logger.error("Find error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(conn);
            }
        }
        return list;
    }

    public static Page<Map<String, Object>> find(String sql, Object[] params, long pageNumber, long pageSize, Connection conn, boolean isCloseConn, Dialect dialect) throws SQLException {
        Page<Map<String, Object>> page = new Page<Map<String, Object>>();
        String pagedSql = dialect.paging(sql, pageNumber, pageSize);
        page.pageNumber = pageNumber;
        page.pageSize = pageSize;
        page.recordTotal = count(sql, params, conn, false, dialect);
        page.pageTotal = (page.recordTotal + pageSize - 1) / pageSize;
        page.objects = find(pagedSql, params, conn, isCloseConn);
        return page;
    }

    public static long count(String sql, Connection conn, boolean isCloseConn, Dialect dialect) throws SQLException {
        return count(sql, null, conn, isCloseConn, dialect);
    }

    public static long count(String sql, Object[] params, Connection conn, boolean isCloseConn, Dialect dialect) throws SQLException {
        String countSql = dialect.count(sql);
        try {
            if (null == params) {
                return (Long) queryRunner.query(conn, countSql, scalarHandler);
            } else {
                return (Long) queryRunner.query(conn, countSql, scalarHandler, params);
            }
        } catch (SQLException e) {
            logger.error("Count error : " + countSql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(conn);
            }
        }
    }

    public static int update(String sql, Object[] params, Connection conn, boolean isCloseConn) throws SQLException {
        try {
            if (null == params) {
                return queryRunner.update(conn, sql);
            } else {
                return queryRunner.update(conn, sql, params);
            }
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException e1) {
                logger.error("Connection error : " + sql, e1);
                throw e1;
            }
            logger.error("Update error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(conn);
            }
        }
    }

    public static int[] batch(String sql, Object[][] params, Connection conn, boolean isCloseConn) throws SQLException {
        try {
            return queryRunner.batch(conn, sql, params);
        } catch (SQLException e) {
            try {
                conn.rollback();
            } catch (SQLException e1) {
                logger.error("Connection error : " + sql, e1);
                throw e1;
            }
            logger.error("Batch error : " + sql, e);
            throw e;
        } finally {
            if (isCloseConn) {
                closeConnection(conn);
            }
        }
    }

    public static List<Meta> getMetaData(String tableName, Connection conn) throws SQLException {
        PreparedStatement st = null;
        ResultSet rs = null;
        try {
            st = conn.prepareStatement("select * from "+ tableName+" where 1=2");
            rs = st.executeQuery();
            ResultSetMetaData meta = rs.getMetaData();
            List<Meta> metas = new ArrayList<Meta>();
            for (int i = 1; i <= meta.getColumnCount(); i++) {
                metas.add(new Meta(meta.getColumnType(i), meta.getColumnName(i), meta.getColumnLabel(i)));
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
            closeConnection(conn);
        }
    }

    private static void closeConnection(Connection conn) throws SQLException {
        if (null != conn) {
            try {
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
            if (obj instanceof BigInteger)
                return ((BigInteger) obj).longValue();
            return obj;
        }
    };

    private static final Logger logger = LoggerFactory.getLogger(DBExecutor.class);

}
