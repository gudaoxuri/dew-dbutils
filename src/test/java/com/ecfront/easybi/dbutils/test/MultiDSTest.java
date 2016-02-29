package com.ecfront.easybi.dbutils.test;

import com.ecfront.easybi.dbutils.exchange.DB;
import com.ecfront.easybi.dbutils.exchange.DS;
import junit.framework.Assert;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MultiDSTest extends DBTest {

    @Test
    public void testMultiDS() throws Exception {
        DB db = new DB();
        db.ddl("create table multi_ds(" +
                "code varchar(255) not null," +
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
        db.batch("insert into multi_ds (code,url,username,password,initialSize,maxActive,minIdle,maxIdle,maxWait,enable) values ( ? , ? , ? , ? , ? , ?, ?, ?, ?, ?, ? )", new Object[][]{
                {"ds1", "jdbc:h2:mem:db1", "sa", "", 10, 50, 5, 20, 5000, true},
                {"ds2", "jdbc:h2:mem:db2", "sa", "", 10, 50, 5, 20, 5000, false}
        });
        Assert.assertEquals(db.count("select * from multi_ds"), 2);

        DS.reload();
        DB multiDB = new DB("ds1");
        testCreateTable(multiDB);
        testUpdate(multiDB);
        Assert.assertEquals(multiDB.count("select * from tuser"), 1);
    }

    @Test
    public void testMultiDSByAPI() throws Exception {
        DS.add("ds1", "jdbc:h2:mem:db1", "sa", "");
        DS.add("ds2", "jdbc:h2:mem:db2", "sa", "");
        DB multiDB = new DB("ds1");
        testCreateTable(multiDB);
        testUpdate(multiDB);
        Assert.assertEquals(multiDB.count("select * from tuser"), 1);
        multiDB = new DB("ds2");
        testCreateTable(multiDB);
        testUpdate(multiDB);
        Assert.assertEquals(multiDB.count("select * from tuser"), 1);
    }


    private static final Logger logger = LoggerFactory.getLogger(MultiDSTest.class);

}
