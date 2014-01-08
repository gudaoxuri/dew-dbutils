package com.ecfront.easybi.dbutils.test;

import com.ecfront.easybi.dbutils.exchange.DB;
import com.ecfront.easybi.dbutils.exchange.DS;
import com.ecfront.easybi.dbutils.exchange.Page;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public class DBTest {

    private void testDropTable(DB db) throws SQLException {
        db.update("drop table user");
    }

    private void testCreateTable(DB db) throws SQLException {
        db.update("create table user(" +
                "id int not null," +
                "name varchar(255)," +
                "password varchar(255)," +
                "age int," +
                "asset decimal," +
                "enable boolean," +
                "primary key(id)" +
                ")");
    }

    private void testUpdate(DB db) throws SQLException {
        db.update("insert into user (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[]{1, "张三", "123", 22, 2333.22, true});
    }

    private void testBatch(DB db) throws SQLException {
        db.batch("insert into user (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[][]{
                {2, "李四", "123", 22, 2333.22, true},
                {3, "王五1", "123", 22, 2333.22, false},
                {4, "王五2", "123", 22, 2333.22, false},
                {5, "王五3", "123", 20, 2333.22, false}
        });
    }

    private void testCount(DB db) throws SQLException {
        long result = db.count("select * from user");
        Assert.assertEquals(result, 5);
    }

    private void testGet(DB db) throws SQLException {
        Map<String, Object> result = db.get("select * from user where id=?", new Object[]{1});
        Assert.assertEquals(result.get("id"), 1);
    }

    private void testFind(DB db) throws SQLException {
        List<Map<String, Object>> result = db.find("select * from user where age=?", new Object[]{22});
        Assert.assertEquals(result.size(), 4);
        Page<Map<String, Object>> page = db.find("select * from user", 1, 2);
        Assert.assertEquals(page.recordTotal, 5);
        Assert.assertEquals(page.pageTotal, 3);

    }

    private void testGetObject(DB db) throws SQLException {
        User user = db.getObject("select * from user where id= ? ", new Object[]{1}, User.class);
        Assert.assertEquals(user.getId(), 1);
    }

    private void testFindObjects(DB db) throws SQLException {
        List<User> users = db.findObjects("select * from user where age=?", new Object[]{22}, User.class);
        Assert.assertEquals(users.size(), 4);

        Page<Map<String, Object>> page = db.find("select * from user", 1, 2);
        Assert.assertEquals(page.recordTotal, 5);
        Assert.assertEquals(page.pageTotal, 3);
    }

    @Test
    public void testDBCP() throws Exception {
        DB db = new DB();
        testCreateTable(db);
        testUpdate(db);
        for (int i = 0; i < 11; i++) {
            testGetObject(db);
        }
        testDropTable(db);
    }

    @Test
    public void testFlow() throws SQLException {
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
        Assert.assertEquals(db.count("select * from user"), 0);

        //error test
        db.open();
        testUpdate(db);
        //has error
        try {
            db.update("insert into user (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[]{1, "张三", "123", 22, 2333.22});
            db.commit();
        } catch (SQLException e) {
            logger.warn("Has Error!");
        }
        Assert.assertEquals(db.count("select * from user"), 0);

        //commit test
        db.open();
        testUpdate(db);
        db.commit();
        Assert.assertEquals(db.count("select * from user"), 1);

        testDropTable(db);
    }

    public void testMultiDS() throws Exception {
        DB db = new DB();
        db.update("create table multi_ds(" +
                "code varchar(255) not null," +
                "driver varchar(255)," +
                "url varchar(255)," +
                "username varchar(255)," +
                "password varchar(255)," +
                "initialSize int," +
                "maxActive int," +
                "minIdle int," +
                "maxIdle int," +
                "maxWait int," +
                "enable int," +
                "primary key(code)" +
                ")");
        db.batch("insert into multi_ds (code,driver,url,username,password,initialSize,maxActive,minIdle,maxIdle,maxWait,enable) values ( ? , ? , ? , ? , ? , ?, ?, ?, ?, ?, ? )", new Object[][]{
                {"ds1", "org.h2.Driver", "jdbc:h2:mem:db1", "sa", "", 10, 50, 5, 20, 5000, true},
                {"ds2", "org.h2.Driver", "jdbc:h2:mem:db2", "sa", "", 10, 50, 5, 20, 5000, false}
        });
        Assert.assertEquals(db.count("select * from multi_ds"), 2);

        DS.reload();
        DB multiDB = new DB("ds1");
        testCreateTable(multiDB);
        testUpdate(multiDB);
        Assert.assertEquals(multiDB.count("select * from user"), 1);
    }

    private static final Logger logger = LoggerFactory.getLogger(DBTest.class);

}
