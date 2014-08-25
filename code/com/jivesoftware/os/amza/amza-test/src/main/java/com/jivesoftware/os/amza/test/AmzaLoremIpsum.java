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
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClient;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfig;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientConfiguration;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactory;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpClientFactoryProvider;
import com.jivesoftware.os.amza.transport.http.replication.client.HttpRequestHelper;
import com.jivesoftware.os.jive.utils.shell.utils.Curl;
import de.svenjacobs.loremipsum.LoremIpsum;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;

public class AmzaLoremIpsum {

    private static final Random rand = new Random();
    private static final LoremIpsum loremIpsum = new LoremIpsum();

    public static void main(String[] args) throws URIException, IOException {

        args = new String[]{"localhost", "1175", "1", "1000000"};

        final String hostName = args[0];
        final int port = Integer.parseInt(args[1]);
        final int firstDocId = Integer.parseInt(args[2]);
        final int count = Integer.parseInt(args[3]);

        String tableName = "lorem";

        for (int i = 0; i < 8; i++) {
            final String tname = tableName + i;
            final int fdi = firstDocId + (count * 1);
            Thread t = new Thread() {
                @Override
                public void run() {
                    try {
                        feed(hostName, port, tname, fdi, count);
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                }
            };
            t.start();
        }
    }

    private static void feed(String hostName, int port, String tableName, int firstDocId, int count) throws URIException, IOException {
        long start = System.currentTimeMillis();
        Curl curl = Curl.create();
        for (int key = 0; key < count; key++) {
            StringBuilder url = new StringBuilder();
            url.append("http://");
            url.append(hostName).append(":").append(port);
            url.append("/example/set");
            url.append("?table=").append(tableName);
            url.append("&key=").append("k" + key);

            String doc = document();
            doc = new String(Base64.encodeBase64(doc.getBytes()));
            url.append("&value=").append(doc);

            String encodedUrl = URIUtil.encodeQuery(url.toString());
            curl.curl(encodedUrl);

            long elapse = System.currentTimeMillis() - start;
            double rate = ((double) elapse / (double) key);
            System.out.println("millisPerAdd:" + rate + " addsPerSec" + (1000d / rate) + " key:" + key);
        }
    }

    public static String document() {
        StringBuilder document = new StringBuilder();
        //document.append("Title-").append(loremIpsum.getWords(rand.nextInt(10), 0)).append(" ");
        //document.append(loremIpsum.getParagraphs(1 + rand.nextInt(20))).append(" ");
        int wordCount = rand.nextInt(1000);
        for (int i = 10; i < wordCount; i++) {
            document.append("booya ");
        }
        return document.toString().replace('\n', ' ');
    }

    static HttpRequestHelper buildRequestHelper(String host, int port) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder().build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        HttpRequestHelper requestHelper = new HttpRequestHelper(httpClient, new ObjectMapper());
        return requestHelper;
    }
}
