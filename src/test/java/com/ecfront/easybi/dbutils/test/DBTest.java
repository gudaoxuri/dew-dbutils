package com.ecfront.easybi.dbutils.test;

import com.ecfront.easybi.dbutils.exchange.DB;
import com.ecfront.easybi.dbutils.exchange.Page;
import junit.framework.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class DBTest {

    @Before
    public void setUp() throws Exception {

    }


    private void testCreateTable() {
        new DB().update("create table user(" +
                "id int not null," +
                "name varchar(255)," +
                "password varchar(255)," +
                "age int," +
                "asset decimal," +
                "enable boolean," +
                "primary key(id)" +
                ")", null);
    }

    private void testUpdate() throws Exception {
        new DB().update("insert into user (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[]{1, "张三", "123", 22, 2333.22, true});
    }


    private void testBatch() throws Exception {
        new DB().batch("insert into user (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )", new Object[][]{
                {2, "李四", "123", 22, 2333.22, true},
                {3, "王五1", "123", 22, 2333.22, false},
                {4, "王五2", "123", 22, 2333.22, false},
                {5, "王五3", "123", 20, 2333.22, false}
        });
    }

    private void testCount() throws Exception {
        long result = new DB().count("select * from user");
        Assert.assertEquals(result, 5);
    }

    private void testGet() throws Exception {
        Map<String, Object> result = new DB().get("select * from user where id=?", new Object[]{1});
        Assert.assertEquals(result.get("id"), 1);
    }

    private void testFind() throws Exception {
        DB db = new DB();
        String sql = "select * from user where age=?";
        List<Map<String, Object>> result = db.find(sql, new Object[]{22});
        Assert.assertEquals(db.count(sql, new Object[]{22}), 4);

        Page<Map<String, Object>> pgae= db.find(sql, null, 1, 2);
        Assert.assertEquals(pgae.recordTotal,4);
        Assert.assertEquals(pgae.pageTotal,3);

    }

    @Test
    public void testFlow() throws Exception {
        testCreateTable();
        testUpdate();
        testBatch();
        testGet();
        testCount();
        testFind();
    }


    @Test
    public void testGetObject() throws Exception {

    }

    @Test
    public void testFindObjects() throws Exception {

    }


    @Test
    public void testOpen() throws Exception {

    }

    @Test
    public void testCommit() throws Exception {

    }

    @Test
    public void testRollback() throws Exception {

    }
}
