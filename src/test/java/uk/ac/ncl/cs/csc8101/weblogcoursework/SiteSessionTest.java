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

import com.clearspring.analytics.stream.cardinality.CardinalityMergeException;
import com.clearspring.analytics.stream.cardinality.HyperLogLog;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.Assert.*;

/**
 * Unit tests for the SiteSession class
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2014-01
 */
public class SiteSessionTest {

    @Before
    public void setup() {
        SiteSession.resetGlobalMax();
    }

    @Test
    public void sessionTest() {

        SiteSession siteSession = new SiteSession("user1", 100, "testURL");
        siteSession.update(200, "testURL2");
        siteSession.update(300, "testURL");
        siteSession.update(400, "testURL2");

        assertEquals("user1", siteSession.getId());
        assertEquals(100, siteSession.getFirstHitMillis() );
        assertEquals(400, siteSession.getLastHitMillis() );
        assertEquals(4, siteSession.getHitCount());

        assertEquals(2, siteSession.getHyperLogLog().cardinality());
    }

    @Test
    public void expiryTest() {

        final AtomicReference<SiteSession> expiredSession = new AtomicReference<>(null);

        HashMap<String,SiteSession> sessions = new LinkedHashMap<String,SiteSession>() {
            protected boolean removeEldestEntry(Map.Entry eldest) {
                SiteSession siteSession = (SiteSession)eldest.getValue();
                boolean shouldExpire = siteSession.isExpired();
                if(shouldExpire) {
                    expiredSession.set(siteSession);
                }
                return siteSession.isExpired();
            }
        };

        SiteSession session = new SiteSession("a", 100, "testURL");
        sessions.put("a", session);
        assertEquals(1, sessions.size());
        assertNull(expiredSession.get());
        sessions.put("b", new SiteSession("b", 101 + SiteSession.MAX_IDLE_MS, "testURL"));
        assertEquals(1, sessions.size());
        assertEquals(session, expiredSession.get());
    }

    @Test
    public void boundaryTest() {
        SiteSession siteSession = new SiteSession("user1", 100, "testURL");
        siteSession.update(100 + SiteSession.MAX_IDLE_MS, "testURL");
    }

    @Test
    public void hllSerializationTest() throws IOException {

        SiteSession siteSession = new SiteSession("user1", 100, "testURL");
        siteSession.update(200, "testURL2");
        siteSession.update(300, "testURL");
        siteSession.update(400, "testURL2");

        long inputCardinality = siteSession.getHyperLogLog().cardinality();

        byte[] inputBytes = siteSession.getHyperLogLog().getBytes();
        ByteBuffer byteBuffer = ByteBuffer.allocate(4+inputBytes.length);
        byteBuffer.putInt(inputBytes.length);
        byteBuffer.put(inputBytes);
        byteBuffer.flip();

        byte[] rebuiltBytes = new byte[byteBuffer.getInt()];
        byteBuffer.get(rebuiltBytes);
        HyperLogLog rebuiltHll = HyperLogLog.Builder.build(rebuiltBytes);

        assertEquals(inputCardinality, rebuiltHll.cardinality());
    }

    @Test
    public void mergeTest() throws CardinalityMergeException {

        SiteSession siteSessionA = new SiteSession("user1", 100, "testURL-A");
        siteSessionA.update(100, "testURL-common");
        SiteSession siteSessionB = new SiteSession("user1", 100, "testURL-B");
        siteSessionB.update(100, "testURL-common");

        HyperLogLog hll = siteSessionA.getHyperLogLog();
        hll.addAll(siteSessionB.getHyperLogLog());

        assertEquals(3, hll.cardinality());
    }
}
