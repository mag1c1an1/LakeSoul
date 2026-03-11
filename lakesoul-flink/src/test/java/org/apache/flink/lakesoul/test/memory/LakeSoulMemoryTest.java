package org.apache.flink.lakesoul.test.memory;

import com.dmetasoul.lakesoul.meta.DBManager;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.lakesoul.metadata.LakeSoulCatalog;
import org.apache.flink.lakesoul.metadata.LakesoulCatalogDatabase;
import org.apache.flink.lakesoul.test.AbstractTestBase;
import org.apache.flink.lakesoul.test.flinkSource.TestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.apache.flink.lakesoul.test.flinkSource.TestUtils.BATCH_TYPE;

public class LakeSoulMemoryTest extends AbstractTestBase {


    String parquetFile = "/data/jiax_space/datautils/massive_rd_data.parquet";

    @Test
    public void memory_leak() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        env.setParallelism(1);
        StreamTableEnvironment tEnvs = StreamTableEnvironment.create(env);
        tEnvs.executeSql(
                "CREATE TABLE users_source (" +
                        "  id BIGINT," +
                        "  `range` STRING" +
                        ") WITH (" +
                        "  'connector' = 'filesystem'," +
                        "  'path' = '" + parquetFile + "'," +
                        "  'format' = 'parquet'" +
                        ")"
        ).await();

        TableResult res = tEnvs.executeSql(
                "select count(*) from users_source"
        );
        res.print();
        Thread.sleep(100000000);
    }

}
