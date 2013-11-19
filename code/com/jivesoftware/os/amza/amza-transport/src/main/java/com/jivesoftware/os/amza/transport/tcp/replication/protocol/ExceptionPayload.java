package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import de.ruedigermoeller.serialization.FSTObjectInput;
import de.ruedigermoeller.serialization.FSTObjectOutput;
import java.io.IOException;

/**
 *
 */
public class ExceptionPayload implements MessagePayload {

    private String message;

    public ExceptionPayload() {
    }

    public ExceptionPayload(String message) {
        this.message = message;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public void serialize(FSTObjectOutput output) throws IOException {
        output.writeStringCompressed(message);
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.message = input.readStringCompressed();
    }
}
