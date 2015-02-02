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

import org.junit.Test;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.*;

/**
 * Unit test for parsing of compressed web server log file
 *
 * @author Jonathan Halliday (jonathan.halliday@redhat.com)
 * @since 2014-01
 */
public class DataReaderTest {

    private static final File dataDir = new File("/home/ubuntu/data/cassandra/");
    // 200m lines, 1,929,934,341 bytes (1.8G)
    private static final File logFile = new File(dataDir, "csc8101_logfile_2015.gz");

    private final DateFormat dateFormat = new SimpleDateFormat("[dd/MMM/yyyy:HH:mm:ss z]");

    @Test
    public void readDataFile() throws IOException, ParseException {

        try (
                final FileInputStream fileInputStream = new FileInputStream(logFile);
                final GZIPInputStream gzipInputStream = new GZIPInputStream(fileInputStream);
                final InputStreamReader inputStreamReader = new InputStreamReader(gzipInputStream);
                final BufferedReader bufferedReader = new BufferedReader(inputStreamReader)
        ) {

            String line = bufferedReader.readLine();
            final String[] tokens = line.split(" ");
            assertEquals(8, tokens.length);

            String dateString = tokens[1]+" "+tokens[2];
            Date date = dateFormat.parse(dateString);
            long millis = date.getTime();
            assertEquals(893971817000L, millis); // 30/Apr/1998:21:30:17 +0000
        }
    }
}
