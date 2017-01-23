package com.jivesoftware.os.amza.client.collection;

import java.nio.charset.Charset;

/**
 * Created by jonathan.colt on 1/23/17.
 */
public class AmzaStringMarshaller implements AmzaMarshaller<String> {

    private final Charset charset;

    public AmzaStringMarshaller(Charset charset) {
        this.charset = charset;
    }


    @Override
    public String fromBytes(byte[] bytes) throws Exception {
        return new String(bytes, charset);
    }

    @Override
    public byte[] toBytes(String value) throws Exception {
        return value.getBytes(charset);
    }
}
