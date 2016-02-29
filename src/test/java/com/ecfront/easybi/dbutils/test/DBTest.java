package com.ecfront.easybi.dbutils.test;

import com.ecfront.easybi.dbutils.exchange.DB;
import com.ecfront.easybi.dbutils.exchange.DS;
import com.ecfront.easybi.dbutils.exchange.Meta;
import com.ecfront.easybi.dbutils.exchange.Page;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class DBTest {

    protected void testDropTable(DB db) throws SQLException {
        db.ddl("drop table tuser");
    }

    protected void testCreateTable(DB db) throws SQLException {
        db.ddl("create table tuser(" +
                "id int not null," +
                "name varchar(255)," +
                "password varchar(255)," +
                "age int," +
                "asset decimal," +
                "enable boolean," +
                "primary key(id)" +
                ")");
    }

    protected void testUpdate(DB db) throws SQLException {
        db.update("insert into tuser (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[]{1, "张三", "123", 22, 2333.22, true});
    }

    protected void testBatch(DB db) throws SQLException {
        db.batch("insert into tuser (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[][]{
                {2, "李四", "123", 22, 2333.22, true},
                {3, "王五1", "123", 22, 2333.22, false},
                {4, "王五2", "123", 22, 2333.22, false},
                {5, "王五3", "123", 20, 2333.22, false}
        });
    }

    protected void testCount(DB db) throws SQLException {
        long result = db.count("select * from tuser");
        Assert.assertEquals(result, 5);
    }

    protected void testGet(DB db) throws SQLException, IOException {
        Map<String, Object> result = db.get("select * from tuser where id=?", new Object[]{1});
        Assert.assertEquals(result.get("id"), 1);
    }

    protected void testFind(DB db) throws SQLException {
        List<Map<String, Object>> result = db.find("select * from tuser where age=?", new Object[]{22});
        Assert.assertEquals(result.size(), 4);
        Page<Map<String, Object>> page = db.find("select * from tuser", 1, 2);
        Assert.assertEquals(page.recordTotal, 5);
        Assert.assertEquals(page.pageTotal, 3);

    }

    protected void testGetObject(DB db) throws SQLException {
        logger.debug("testGetObject.");
        User user = db.getObject("select * from tuser where id= ? ", new Object[]{1}, User.class);
        Assert.assertEquals(user.getId(), 1);
    }

    protected void testFindObjects(DB db) throws SQLException {
        List<User> users = db.findObjects("select * from tuser where age=?", new Object[]{22}, User.class);
        Assert.assertEquals(users.size(), 4);

        Page<Map<String, Object>> page = db.find("select * from tuser", 1, 2);
        Assert.assertEquals(page.recordTotal, 5);
        Assert.assertEquals(page.pageTotal, 3);
    }

    @Test
    public void testMeta() throws Exception {
        DB db = new DB();
        testCreateTable(db);
        List<Meta> metas = db.getMetaData("tuser");
        Assert.assertEquals(metas.get(0).label, "id");
        Meta meta = db.getMetaData("tuser", "name");
        Assert.assertEquals(meta.label, "name");
        testDropTable(db);
    }

    @Test
    public void testPool() throws Exception {
        DB db = new DB();
        testCreateTable(db);
        testUpdate(db);
        testBatch(db);
        final CountDownLatch watch = new CountDownLatch(10000);
        final AtomicInteger count = new AtomicInteger(0);
        for (int i = 0; i < 100; i++) {
            new Thread(() -> {
                for (int i1 = 0; i1 < 100; i1++) {
                    try {
                        logger.debug(">>>>>>>>>>>>>>" + count.incrementAndGet());
                        watch.countDown();
                        testFind(new DB());
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }).start();
        }
        watch.await();
        testDropTable(db);
    }

    @Test
    public void testFlow() throws SQLException, IOException {
        DB db = new DB();
        testCreateTable(db);
        testUpdate(db);
        testBatch(db);
        testGet(db);
        testCount(db);
        testFind(db);

        testGetObject(db);
        testFindObjects(db);

        testDropTable(db);
    }

    @Test
    public void testTransaction() throws SQLException {
        DB db = new DB();
        testCreateTable(db);
        //rollback test
        db.open();
        testUpdate(db);
        db.rollback();
        Assert.assertEquals(db.count("select * from tuser"), 0);

        //error test
        db.open();
        testUpdate(db);
        //has error
        try {
            db.update("insert into tuser (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[]{1, "张三", "123", 22, 2333.22});
            db.commit();
        } catch (SQLException e) {
            logger.warn("Has Error!");
        }
        Assert.assertEquals(db.count("select * from tuser"), 0);

        //commit test
        db.open();
        testUpdate(db);
        db.commit();
        Assert.assertEquals(db.count("select * from tuser"), 1);

        testDropTable(db);
    }

    @Test
    public void testCreateAndUpdate() throws SQLException, IOException {
        String testPath = this.getClass().getResource("/").getPath();
        if (System.getProperties().getProperty("os.name").toUpperCase().contains("WINDOWS")) {
            testPath = testPath.substring(1);
        }
        DS.setConfigPath(testPath);
        DB db = new DB();
        Map<String, String> fields = new HashMap<>();
        fields.put("id", "long");
        fields.put("name", "String");
        fields.put("age", "Int");
        fields.put("addr", "String");
        fields.put("height1", "Float");
        fields.put("height2", "Double");
        fields.put("createTime", "Date");
        fields.put("asset", "BigDecimal");
        fields.put("addr", "String");
        fields.put("enable", "Boolean");
        fields.put("txt", "text");
        db.createTableIfNotExist("test", "测试表", fields, new HashMap<String, String>() {{
            put("name", "姓名");
            put("age", "年龄");
        }}, new ArrayList<String>() {{
            add("name");
        }}, new ArrayList<String>() {{
            add("name");
        }}, "id");
        Map<String, Object> values = new HashMap<>();
        values.put("id", 100);
        values.put("name", "gudaoxuri");
        values.put("age", 29);
        values.put("height1", 1.1);
        values.put("height2", 1.1d);
        values.put("asset", new BigDecimal(2.343));
        values.put("enable", true);
        values.put("addr", "浙江杭州");
        //  values.put("createTime", new java.sql.Date());
        values.put("txt", "浙江杭州");
        db.save("test", values);
        values.put("name", "孤岛旭日");
        db.update("test", 100, values);
        Map<String, Object> res = db.getByPk("test", 100);
        Assert.assertEquals(res.get("name"), "孤岛旭日");
        Assert.assertEquals(res.get("age"), 29);
        Assert.assertEquals(res.get("height1"), 1.1);
        Assert.assertEquals(res.get("height2"), 1.1);
        Assert.assertEquals(res.get("addr"), "浙江杭州");
        Assert.assertEquals(res.get("txt"), "浙江杭州");
        db.deleteByPk("test", 100);
        Assert.assertEquals(db.getByPk("test", 100), null);
    }

    private static final Logger logger = LoggerFactory.getLogger(DBTest.class);

}
