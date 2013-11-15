package com.jivesoftware.os.amza.test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.jivesoftware.os.jive.utils.http.client.HttpClient;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfig;
import com.jivesoftware.os.jive.utils.http.client.HttpClientConfiguration;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactory;
import com.jivesoftware.os.jive.utils.http.client.HttpClientFactoryProvider;
import com.jivesoftware.os.jive.utils.http.client.rest.RequestHelper;
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

        for (int i = 0; i < 2; i++) {
            final String tname = tableName + i;
            new Thread() {

                @Override
                public void run() {
                    try {
                        feed(hostName, port, tname, firstDocId, count);
                    } catch (Exception x) {
                        x.printStackTrace();
                    }
                }
            }.start();
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
            url.append("&key=").append((firstDocId + key));

            String doc = document();
            doc = new String(Base64.encodeBase64(doc.getBytes()));
            url.append("&value=").append(doc);

            String encodedUrl = URIUtil.encodeQuery(url.toString());
            curl.curl(encodedUrl);

            long elapse = System.currentTimeMillis() - start;
            double rate = ((double) elapse / (double) key);
            System.out.println("mpi:" + rate + " millis " + (1000d / rate) + " ips " + key);
        }
    }

    public static String document() {
        StringBuilder document = new StringBuilder();
        document.append("Title-").append(loremIpsum.getWords(rand.nextInt(10), 0)).append(" ");
        document.append(loremIpsum.getParagraphs(1 + rand.nextInt(20))).append(" ");
        return document.toString().replace('\n', ' ');
    }

    static RequestHelper buildRequestHelper(String host, int port) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder().build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        RequestHelper requestHelper = new RequestHelper(httpClient, new ObjectMapper());
        return requestHelper;
    }
}