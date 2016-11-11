package com.jivesoftware.os.amza.sync.deployable;

import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelper;
import com.jivesoftware.os.routing.bird.http.client.HttpRequestHelperUtils;
import com.jivesoftware.os.routing.bird.http.client.OAuthSigner;
import oauth.signpost.commonshttp.CommonsHttpOAuthConsumer;
import oauth.signpost.signature.HmacSha1MessageSigner;
import org.apache.commons.lang.StringUtils;

/**
 *
 */
public class AmzaSyncClientInitializer {

    public AmzaSyncClient initialize(AmzaSyncConfig config) throws Exception {

        String consumerKey = StringUtils.trimToNull(config.getSyncSenderOAuthConsumerKey());
        String consumerSecret = StringUtils.trimToNull(config.getSyncSenderOAuthConsumerSecret());
        String consumerMethod = StringUtils.trimToNull(config.getSyncSenderOAuthConsumerMethod());
        if (consumerKey == null || consumerSecret == null || consumerMethod == null) {
            throw new IllegalStateException("OAuth consumer has not been configured");
        }

        consumerMethod = consumerMethod.toLowerCase();
        if (!consumerMethod.equals("hmac") && !consumerMethod.equals("rsa")) {
            throw new IllegalStateException("OAuth consumer method must be one of HMAC or RSA");
        }

        String schemeHostPort = config.getSyncSenderSchemeHostPort();

        String[] parts = schemeHostPort.split(":");
        String scheme = parts[0];
        String host = parts[1];
        int port = Integer.parseInt(parts[2]);

        boolean sslEnable = scheme.equals("https");
        OAuthSigner authSigner = (request) -> {
            CommonsHttpOAuthConsumer oAuthConsumer = new CommonsHttpOAuthConsumer(consumerKey, consumerSecret);
            oAuthConsumer.setMessageSigner(new HmacSha1MessageSigner());
            oAuthConsumer.setTokenWithSecret(consumerKey, consumerSecret);
            return oAuthConsumer.sign(request);
        };
        HttpRequestHelper requestHelper = HttpRequestHelperUtils.buildRequestHelper(sslEnable,
            config.getSyncSenderAllowSelfSignedCerts(),
            authSigner,
            host,
            port,
            config.getSyncSenderSocketTimeout());

        return new HttpAmzaSyncClient(requestHelper,
            "/api/sync/v1/commit/rows",
            "/api/sync/v1/ensure/partition");
    }
}
