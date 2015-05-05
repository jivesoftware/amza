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

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.http.HttpResponse;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;

public class AmzaSetStress {

    public static final String CONTENT_TYPE_HEADER_NAME = "Content-Type";
    public static final String APPLICATION_JSON_CONTENT_TYPE = "application/json";
    public static final String APPLICATION_OCTET_STREAM_TYPE = "application/octet-stream";
    public static final ObjectMapper MAPPER = new ObjectMapper();

    public static void main(String[] args) throws IOException {

        args = new String[]{"soa-integ-data12.phx1.jivehosted.com", "1185", "1", "10000"};

        final String hostName = args[0];
        final int port = Integer.parseInt(args[1]);
        final int firstDocId = Integer.parseInt(args[2]);
        final int count = Integer.parseInt(args[3]);
        final int batchSize = 100;

        String regionName = "lorem";

        for (int i = 0; i < 8; i++) {
            final String rname = regionName + i;
            final org.apache.http.client.HttpClient httpClient = HttpClients.createDefault();

            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        feed(httpClient, hostName, port, rname, 0, count, batchSize);
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                }
            };
            t.start();
        }
    }

    private static void feed(org.apache.http.client.HttpClient httpClient,
        String hostName, int port, String regionName, int firstDocId, int count, int batchSize) throws IOException, InterruptedException {
        long start = System.currentTimeMillis();
        for (int key = firstDocId; key < count; key++) {
            StringBuilder url = new StringBuilder();
            url.append("http://");
            url.append(hostName).append(":").append(port);
            url.append("/amza/multiSet");
            url.append("/").append(regionName);

            Map<String, String> values = new LinkedHashMap<>();
            for (int b = 0; b < batchSize; b++) {

                values.put(b + "k" + key, b + "v" + key);
            }

            String postJsonBody = MAPPER.writeValueAsString(values);
            while (true) {
                HttpPost method = new HttpPost(url.toString());
                method.setEntity(new StringEntity(postJsonBody, ContentType.APPLICATION_JSON));
                method.setHeader(CONTENT_TYPE_HEADER_NAME, APPLICATION_JSON_CONTENT_TYPE);

                StatusLine statusLine;
                try {
                    try {
                        HttpResponse response = httpClient.execute(method);
                        statusLine = response.getStatusLine();
                        if (statusLine.getStatusCode() == 200) {
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
                System.out.println(regionName + " millisPerAdd:" + millisPerAdd + " addsPerSec:" + (1000d / millisPerAdd) + " key:" + key);
            }
        }
    }

    public static String document(long id) {
        StringBuilder document = new StringBuilder();
        document.append(id);
        return document.toString();
    }

}
