package com.ecfront.easybi.dbutils.exchange;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <h1>并发版本的数据操作类</h1>
 */
public class ConcurrentDB {

      public ConcurrentDB(DB db){
          this.db=db;
      }

    /**
     * DDL操作
     * @param ddls  DDL语句
     */
    public void ddls(final List<String> ddls) throws SQLException{
        final CountDownLatch signal = new CountDownLatch(ddls.size());
        for (int i = 0; i < ddls.size(); i++) {
            final int finalI = i;
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        db.ddl(ddls.get(finalI));
                    } catch (SQLException e) {
                        logger.warn("ddl execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
    }

    /**
     * 获取多个对象
     *
     * @param sqls   SQLs
     * @param clazz 对象类
     * @return java对象集合
     */
    public <E> Map<String,E> getObjects(final Map<String,String> sqls, final Class<E> clazz) throws SQLException, ExecutionException, InterruptedException {
        return getObjects(sqls,null,clazz);
    }

    /**
     * 获取多个对象
     *
     * @param sqls    SQLs
     * @param params 参数
     * @param clazz  对象类
     * @return java对象集合
     */
    public <E> Map<String, E> getObjects(final Map<String, String> sqls, final Object[] params,final Class<E> clazz) throws SQLException {
        final Map<String, E> result = new HashMap<String,E>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for(final Map.Entry<String,String> entry:sqls.entrySet()){
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.getObject(entry.getValue(), params, clazz));
                    } catch (SQLException e) {
                        logger.warn("getObjects execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    /**
     * 获取多个对象集合
     *
     * @param sqls   SQLs
     * @param clazz 对象类
     * @return java对象集合
     */
    public <E> Map<String, List<E>> findObjects(final Map<String, String> sqls,final Class<E> clazz) throws SQLException {
        return findObjects(sqls, null, clazz);
    }

    /**
     * 获取多个对象集合
     *
     * @param sqls    SQLs
     * @param params 参数
     * @param clazz  对象类
     * @return java对象集合
     */
    public <E> Map<String, List<E>> findObjects(final Map<String, String> sqls, final Object[] params, final Class<E> clazz) throws SQLException {
        final Map<String, List<E>> result = new HashMap<String, List<E>>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.findObjects(entry.getValue(), params, clazz));
                    } catch (SQLException e) {
                        logger.warn("findObjects execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    /**
     * 获取多个对象集合（带分页）
     *
     * @param sqls        SQLs
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @param clazz      对象类
     * @return java对象集合（带分页）
     */
    public <E> Map<String, Page<E>> findObjects(final Map<String, String> sqls,long pageNumber, long pageSize, Class<E> clazz) throws SQLException {
        return findObjects(sqls, null, pageNumber, pageSize, clazz);
    }

    /**
     * 获取多个对象集合（带分页）
     *
     * @param sqls        SQLs
     * @param params     参数
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @param clazz      对象类
     * @return java对象集合（带分页）
     */
    public <E> Map<String, Page<E>> findObjects(final Map<String, String> sqls,final Object[] params,final long pageNumber, final long pageSize,final Class<E> clazz) throws SQLException {
        final Map<String, Page<E>> result = new HashMap<String, Page<E>>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.findObjects(entry.getValue(), params, pageNumber, pageSize, clazz));
                    } catch (SQLException e) {
                        logger.warn("findObjects execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    /**
     * 获取单条记录集合
     *
     * @param sqls SQLs
     * @return 单条记录集合
     */
    public Map<String, Map<String, Object>> get(final Map<String, String> sqls) throws SQLException {
        return get(sqls, null);
    }

    /**
     * 获取单条记录集合
     *
     * @param sqls    SQLs
     * @param params 参数
     * @return 单条记录集合
     */
    public Map<String, Map<String, Object>> get(final Map<String, String> sqls,final Object[] params) throws SQLException {
        final Map<String, Map<String, Object>> result = new HashMap<String, Map<String, Object>>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.get(entry.getValue(), params));
                    } catch (SQLException e) {
                        logger.warn("get execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }


    /**
     * 获取多条记录集合（带分页）
     *
     * @param sqls SQLs
     * @return 多条记录集合（带分页）
     */
    public Map<String, List<Map<String, Object>>> find(final Map<String, String> sqls) throws SQLException {
        return find(sqls, null);
    }

    /**
     * 获取多条记录集合（带分页）
     *
     * @param sqls    SQLs
     * @param params 参数
     * @return 多条记录集合（带分页）
     */
    public Map<String, List<Map<String, Object>>> find(final Map<String, String> sqls,final Object[] params) throws SQLException {
        final Map<String, List<Map<String, Object>>> result = new HashMap<String, List<Map<String, Object>>>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.find(entry.getValue(), params));
                    } catch (SQLException e) {
                        logger.warn("find execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    /**
     * 获取多条记录集合（带分页）
     *
     * @param sqls        SQLs
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @return 多条记录集合（带分页）
     */
    public Map<String, Page<Map<String, Object>>> find(final Map<String, String> sqls, int pageNumber, int pageSize) throws SQLException {
        return find(sqls, null, pageNumber, pageSize);
    }


    /**
     * 获取多条记录集合（带分页）
     *
     * @param sqls        SQLs
     * @param params     参数
     * @param pageNumber 页码（从1开始）
     * @param pageSize   每页条数
     * @return 多条记录集合（带分页）
     */
    public Map<String, Page<Map<String, Object>>> find(final Map<String, String> sqls,final Object[] params, final int pageNumber, final int pageSize) throws SQLException {
        final Map<String, Page<Map<String, Object>>> result = new HashMap<String, Page<Map<String, Object>>>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.find(entry.getValue(), params,pageNumber,pageSize));
                    } catch (SQLException e) {
                        logger.warn("find execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    /**
     * 获取记录数集合
     *
     * @param sqls SQLs
     * @return 记录数集合
     */
    public Map<String, Long> count(final Map<String, String> sqls) throws SQLException {
        return count(sqls, null);
    }

    /**
     * 获取记录数集合
     *
     * @param sqls    SQLs
     * @param params 参数
     * @return 记录数集合
     */
    public Map<String, Long> count(final Map<String, String> sqls,final Object[] params) throws SQLException {
        final Map<String, Long> result = new HashMap<String, Long>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.count(entry.getValue(), params));
                    } catch (SQLException e) {
                        logger.warn("count execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    /**
     * 更新记录集合
     *
     * @param sqls SQLs
     */
    public Map<String, Integer> update(final Map<String, String> sqls) throws SQLException {
        return update(sqls, null);
    }

    /**
     * 更新记录集合
     *
     * @param sqls    SQLs
     * @param params 参数
     */
    public Map<String, Integer> update(final Map<String, String> sqls,final Object[] params) throws SQLException {
        final Map<String, Integer> result = new HashMap<String, Integer>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.update(entry.getValue(), params));
                    } catch (SQLException e) {
                        logger.warn("update execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    /**
     * 批量更新记录集合
     *
     * @param sqls    SQLs
     * @param params 参数
     */
    public Map<String, int[]> batch(final Map<String, String> sqls,final Object[][] params) throws SQLException {
        final Map<String, int[]> result = new HashMap<String, int[]>();
        final CountDownLatch signal = new CountDownLatch(sqls.size());
        for (final Map.Entry<String, String> entry : sqls.entrySet()) {
            executorService.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        result.put(entry.getKey(), db.batch(entry.getValue(), params));
                    } catch (SQLException e) {
                        logger.warn("batch execute error..", e);
                    } finally {
                        signal.countDown();
                    }
                }
            });
        }
        try {
            signal.await();
        } catch (InterruptedException e) {
            logger.warn("MultiThread Interrupted.", e);
        }
        return result;
    }

    private DB db;
    private static ExecutorService  executorService= Executors.newCachedThreadPool();

    private static final Logger logger = LoggerFactory.getLogger(ConcurrentDB.class);


}
