/*
 * Copyright 2013 Jive Software, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.jivesoftware.os.amza.test;

import de.svenjacobs.loremipsum.LoremIpsum;
import java.io.IOException;
import java.util.Random;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.StatusLine;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.commons.httpclient.util.URIUtil;

public class AmzaGetStress {

    private static final Random rand = new Random();
    private static final LoremIpsum loremIpsum = new LoremIpsum();

    public static void main(String[] args) throws URIException, IOException {

        args = new String[]{"localhost", "1185", "1", "10000"};

        final String hostName = args[0];
        final int port = Integer.parseInt(args[1]);
        final int firstDocId = Integer.parseInt(args[2]);
        final int count = Integer.parseInt(args[3]);
        final int batchSize = 100;

        String regionName = "lorem";

        for (int i = 0; i < 1; i++) {
            final String rname = regionName + i;
            MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
            final org.apache.commons.httpclient.HttpClient httpClient = new org.apache.commons.httpclient.HttpClient(connectionManager);

            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        get(httpClient, hostName, port, rname, 0, count, batchSize);
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                }
            };
            t.start();
        }
    }

    private static void get(org.apache.commons.httpclient.HttpClient httpClient,
        String hostName, int port, String regionName, int firstDocId, int count, int batchSize) throws URIException, IOException, InterruptedException {
        long start = System.currentTimeMillis();
        for (int key = firstDocId; key < count; key++) {
            StringBuilder url = new StringBuilder();
            url.append("http://");
            url.append(hostName).append(":").append(port);
            url.append("/amza/get");
            url.append("?region=").append(regionName);
            url.append("&key=");

            for (int b = 0; b < batchSize; b++) {
                if (b > 0) {
                    url.append(',');
                }
                url.append(b + "k" + key);
            }

            String encodedUrl = URIUtil.encodeQuery(url.toString());
            while (true) {
                GetMethod method = new GetMethod(encodedUrl);
                StatusLine statusLine;
                try {
                    try {
                        httpClient.executeMethod(method);

                        statusLine = method.getStatusLine();
                        if (statusLine.getStatusCode() == 200) {
                            System.out.println("Got:" + new String(method.getResponseBody()));
                            break;
                        }
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                    Thread.sleep(1000);
                } finally {
                    method.releaseConnection();
                }
            }

            if (key % 100 == 0) {
                long elapse = System.currentTimeMillis() - start;
                double millisPerAdd = ((double) elapse / (double) key);
                System.out.println(regionName + " millisPerGet:" + millisPerAdd + " getsPerSec:" + (1000d / millisPerAdd) + " key:" + key);
            }
        }
    }

}
