package org.apache.flink.lakesoul.test;

import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.metadata.LakesoulCatalogDatabase;
import org.apache.flink.lakesoul.table.LakeSoulCatalogFactory;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Catalog;
import org.junit.Before;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.junit.Assert.assertTrue;

public class PartitionTest extends AbstractTestBase {

    private final String LAKESOUL = "lakesoul";
    private Map<String, String> props;
    private StreamTableEnvironment tEnvs;
    private DBManager DbManage;

    @Before
    public void before() {
        props = new HashMap<>();
        props.put("type", LAKESOUL);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        tEnvs = StreamTableEnvironment.create(env);
        LakeSoulCatalog lakesoulCatalog = new LakeSoulCatalog();
//        lakesoulCatalog.cleanForTest();
        lakesoulCatalog.open();

        lakesoulCatalog.createDatabase("test_lakesoul_meta", new LakesoulCatalogDatabase(), true);
        tEnvs.registerCatalog(LAKESOUL, lakesoulCatalog);
        tEnvs.useCatalog(LAKESOUL);
        tEnvs.useDatabase("test_lakesoul_meta");
        DbManage = new DBManager();
    }

    @Test
    public void t() throws ExecutionException, InterruptedException {
        tEnvs.executeSql("CREATE TABLE IF NOT EXISTS x (" +
                "  id INT, " +
                "  d DATE, " +
                "  PRIMARY KEY (id) NOT ENFORCED" + // 必须指定主键
                ") " +
                "PARTITIONED BY (d) " +            // 分区字段必须是表字段之一
                "WITH (" +
                "  'connector' = 'lakesoul'," +      // 修正括号闭合
                "  'hashBucketNum' = '4'," +      // 修正括号闭合
                "    'path'='/tmp/lakesoul/test'" +
                ")").await();
        tEnvs.executeSql("insert into x values(1,CAST(NULL AS DATE))").await();
        tEnvs.executeSql("select * from x").print();
        tEnvs.executeSql("ALTER TABLE x DROP PARTITION (d = 'NULL');").print();
    }
}
