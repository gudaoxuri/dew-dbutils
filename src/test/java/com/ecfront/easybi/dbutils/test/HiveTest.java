package com.ecfront.easybi.dbutils.test;


import com.ecfront.easybi.dbutils.exchange.ConcurrentDB;
import com.ecfront.easybi.dbutils.exchange.DB;
import com.ecfront.easybi.dbutils.exchange.Meta;
import junit.framework.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HiveTest {

    @Test
    public void testMeta() throws Exception {
        DB db = new DB();
        List<Meta> metas = db.getMetaData("tuser");
        Assert.assertEquals(metas.get(0).label, "id");
        Meta meta = db.getMetaData("tuser", "name");
        Assert.assertEquals(meta.label, "name");
    }

    //  @Test
    public void test() throws Exception {
        DB db = new DB();
        db.ddl("create table test_jzy like test.spc_mkt_browse_label_10k");
        List<Map<String, Object>> result = db.find("select\n" +
                "sum(case when price<10000 then 1 else 0 end),\n" +
                "sum(case when price>= 10000 and price<20000 then 1 else 0 end),\n" +
                "sum(case when price>=20000 then 1 else 0 end)\n" +
                "from new_mkt_browse_label_10k");
        db.ddl("drop table test_jzy");
    }

    //   @Test
    public void testConcurrent() throws Exception {
        ConcurrentDB cdb = new ConcurrentDB(new DB());
        Map<String, String> sqls = new HashMap<>();
        sqls.put("江干统计", "select count(1) from test.spc_mkt_browse_label_10k where area='江干'");
        sqls.put("西湖统计", "select count(1) from test.spc_mkt_browse_label_10k where area='西湖'");
        sqls.put("上城统计", "select count(1) from test.spc_mkt_browse_label_10k where area='上城'");
        Map<String, List<Map<String, Object>>> result = cdb.find(sqls);
    }

}
