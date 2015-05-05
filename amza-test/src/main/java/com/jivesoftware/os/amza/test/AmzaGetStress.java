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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.google.common.io.BaseEncoding;
import de.svenjacobs.loremipsum.LoremIpsum;
import java.io.IOException;
import java.util.Random;
import java.util.Set;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

public class AmzaGetStress {

    private static final Random rand = new Random();
    private static final LoremIpsum loremIpsum = new LoremIpsum();
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void main(String[] args) throws IOException {

        args = new String[]{"soa-integ-data11.phx1.jivehosted.com", "1185", "1", "10000"};

        final String hostName = args[0];
        final int port = Integer.parseInt(args[1]);
        final int firstDocId = Integer.parseInt(args[2]);
        final int count = Integer.parseInt(args[3]);
        final int batchSize = 100;

        String regionName = "lorem";

        for (int i = 0; i < 1024; i++) {
            final String rname = regionName + i;
            final org.apache.http.client.HttpClient httpClient = HttpClients.createDefault();

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

    private static void get(org.apache.http.client.HttpClient httpClient,
        String hostName, int port, String regionName, int firstDocId, int count, int batchSize) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        for (int key = firstDocId; key < count; key++) {
            StringBuilder url = new StringBuilder();
            url.append("http://");
            url.append(hostName).append(":").append(port);
            url.append("/amza/get");
            url.append("?region=").append(regionName);
            url.append("&key=");

            Set<String> expectedValues = Sets.newHashSet();
            for (int b = 0; b < batchSize; b++) {
                if (b > 0) {
                    url.append(',');
                }
                url.append(b).append('k').append(key);
                expectedValues.add(b + "v" + key);
            }

            while (true) {
                HttpGet method = new HttpGet(url.toString());
                StatusLine statusLine;
                try {
                    try {
                        HttpResponse response = httpClient.execute(method);

                        statusLine = response.getStatusLine();
                        if (statusLine.getStatusCode() == 200) {
                            //System.out.println("Got:" + new String(method.getResponseBody()));
                            ArrayNode node = mapper.readValue(EntityUtils.toString(response.getEntity()), ArrayNode.class);
                            for (JsonNode value : node) {
                                if (!value.isNull()) {
                                    expectedValues.remove(new String(BaseEncoding.base64().decode(value.textValue()), Charsets.UTF_8));
                                }
                            }
                            if (!expectedValues.isEmpty()) {
                                System.out.println("Missing values in " + regionName + " for key " + key + ": " + expectedValues);
                            }
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
