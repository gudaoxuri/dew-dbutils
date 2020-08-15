/*
 * Copyright 2020. the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ecfront.dew.dbutils.test;

import com.ecfront.dew.dbutils.DewDBUtils;
import com.ecfront.dew.dbutils.dto.DSConfig;
import com.ecfront.dew.dbutils.DewDB;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;

public class MultiDSTest {

    @Test
    public void testMultiDS() throws Exception {
        DewDBUtils.init(this.getClass().getResource("/").getPath() + File.separator + "config.yml");
        DewDB db = DewDBUtils.use("default");
        db.ddl("create table multi_ds(" +
                "code varchar(255) not null," +
                "url varchar(255)," +
                "username varchar(255)," +
                "password varchar(255)," +
                "pool_initialSize int," +
                "pool_maxActive int," +
                "monitor int," +
                "primary key(code)" +
                ")");
        db.batch("insert into multi_ds (code,url,username,password,pool_initialSize,pool_maxActive,monitor) values ( ? , ? , ? , ? , ?, ?, ? )",
                new Object[][]{
                        {"ds1", "jdbc:h2:mem:db1", "sa", "", 2, 5, 0},
                        {"ds2", "jdbc:h2:mem:db2", "sa", "", 2, 10, 0}
                });
        Assert.assertEquals(2, db.count("select * from multi_ds"));

        DewDBUtils.init(this.getClass().getResource("/").getPath() + File.separator + "config-dynamic.yml");

        DewDB multiDB = DewDBUtils.use("ds2");
        multiDB.ddl("create table tuser(" +
                "id int not null," +
                "name varchar(255)," +
                "password varchar(255)," +
                "age int," +
                "asset decimal," +
                "enable boolean," +
                "primary key(id)" +
                ")");
        multiDB.find("select * from tuser");
        multiDB.update("insert into tuser (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )",
                1, "张三", "123", 22, 2333.22, true);
        Assert.assertEquals(1, multiDB.count("select * from tuser"));
        multiDB.ddl("drop table tuser");

        DewDBUtils.addDS(DSConfig.builder()
                .code("ds3")
                .url("jdbc:h2:mem:db3")
                .username("sa")
                .password("")
                .monitor(false)
                .pool(DSConfig.PoolConfig.builder()
                        .initialSize(2)
                        .maxActive(5)
                        .build()).build());
        multiDB = DewDBUtils.use("ds3");
        multiDB.ddl("create table tuser(" +
                "id int not null," +
                "name varchar(255)," +
                "password varchar(255)," +
                "age int," +
                "asset decimal," +
                "enable boolean," +
                "primary key(id)" +
                ")");
        multiDB.find("select * from tuser");
        multiDB.update("insert into tuser (id,name,password,age,asset,enable) values ( ? , ? , ? , ? , ? , ? )",
                1, "张三", "123", 22, 2333.22, true);
        Assert.assertEquals(1, multiDB.count("select * from tuser"));
        multiDB.ddl("drop table tuser");

    }

}
