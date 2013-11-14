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
import org.apache.commons.httpclient.URIException;
import org.apache.commons.httpclient.util.URIUtil;

public class AmzaLoremIpsum {

    private static final Random rand = new Random();
    private static final LoremIpsum loremIpsum = new LoremIpsum();

    public static void main(String[] args) throws URIException, IOException {

        args = new String[]{"localhost","1175","1","10"};

        String hostName = args[0];
        int port = Integer.parseInt(args[1]);
        int firstDocId = Integer.parseInt(args[2]);
        int count = Integer.parseInt(args[3]);

        String tableName = "table1";

        Curl curl = Curl.create();
        for (int key = 0; key < count; key++) {
            StringBuilder url = new StringBuilder();
            url.append("http://");
            url.append(hostName).append(":").append(port);
            url.append("/example/set");
            url.append("?table=").append(tableName);
            url.append("&key=").append((firstDocId + key));
            url.append("&value=").append(document());

            String encodedUrl = URIUtil.encodeQuery(url.toString());
            curl.curl(encodedUrl);
        }
    }

    public static String document() {
        StringBuilder document = new StringBuilder();
        document.append("Title:").append(loremIpsum.getWords(rand.nextInt(10), 0)).append("\n\n");
        document.append(loremIpsum.getParagraphs(1 + rand.nextInt(20))).append("\n");
        return document.toString();
    }

    static RequestHelper buildRequestHelper(String host, int port) {
        HttpClientConfig httpClientConfig = HttpClientConfig.newBuilder().build();
        HttpClientFactory httpClientFactory = new HttpClientFactoryProvider().createHttpClientFactory(Arrays.<HttpClientConfiguration>asList(httpClientConfig));
        HttpClient httpClient = httpClientFactory.createClient(host, port);
        RequestHelper requestHelper = new RequestHelper(httpClient, new ObjectMapper());
        return requestHelper;
    }
}