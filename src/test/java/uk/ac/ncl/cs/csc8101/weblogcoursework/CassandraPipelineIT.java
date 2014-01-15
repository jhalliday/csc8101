/*
Copyright 2014 Red Hat, Inc. and/or its affiliates.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package uk.ac.ncl.cs.csc8101.weblogcoursework;

import com.datastax.driver.core.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Write pipelining tests for cassandra server v2 / CQL3 via datastax java-driver
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2014-01
 */
public class CassandraPipelineIT {

    private static Cluster cluster;
    private static Session session;

    @BeforeClass
    public static void staticSetup() {

        cluster = new Cluster.Builder()
                .addContactPoint("127.0.0.1")
                .build();

        final int numberOfConnections = 1;

        PoolingOptions poolingOptions = cluster.getConfiguration().getPoolingOptions();
        poolingOptions.setCoreConnectionsPerHost(HostDistance.LOCAL, numberOfConnections);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.LOCAL, numberOfConnections);
        poolingOptions.setCoreConnectionsPerHost(HostDistance.REMOTE, numberOfConnections);
        poolingOptions.setMaxConnectionsPerHost(HostDistance.REMOTE, numberOfConnections);

        final Session bootstrapSession = cluster.connect();
        bootstrapSession.execute("CREATE KEYSPACE IF NOT EXISTS test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 }");
        bootstrapSession.shutdown();

        session = cluster.connect("test");

        session.execute("CREATE TABLE IF NOT EXISTS test_data_table (k bigint, v text, PRIMARY KEY (k) )");
    }

    @AfterClass
    public static void staticCleanup() {
        session.shutdown();
        cluster.shutdown();
    }

    @Test
    public void pipelineWrites() throws InterruptedException {

        final PreparedStatement insertPS = session.prepare("INSERT INTO test_data_table (k, v) VALUES (?, ?)");

        final int numberOfBatches = 10;
        final int itemsPerBatch = 5;

        final int maxOutstandingFutures = 4;
        final BlockingQueue<ResultSetFuture> outstandingFutures = new LinkedBlockingQueue<>(maxOutstandingFutures);

        for(int i = 0; i < numberOfBatches; i++) {

            final BatchStatement batchStatement = new BatchStatement(BatchStatement.Type.UNLOGGED);
            for(int j = 0; j < itemsPerBatch; j++) {
                batchStatement.add( new BoundStatement(insertPS).bind((long)((i*itemsPerBatch)+j), "batch-item-"+i+"-"+j));
            }

            outstandingFutures.put(session.executeAsync(batchStatement));

            if(outstandingFutures.remainingCapacity() == 0) {
                ResultSetFuture resultSetFuture = outstandingFutures.take();
                resultSetFuture.getUninterruptibly();
            }
        }

        while(!outstandingFutures.isEmpty()) {
            ResultSetFuture resultSetFuture = outstandingFutures.take();
            resultSetFuture.getUninterruptibly();
        }
    }
}
