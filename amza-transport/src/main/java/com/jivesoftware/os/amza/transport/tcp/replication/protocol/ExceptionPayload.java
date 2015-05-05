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
package com.jivesoftware.os.amza.transport.tcp.replication.protocol;

import com.jivesoftware.os.amza.transport.tcp.replication.serialization.MessagePayload;
import java.io.IOException;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

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
        output.writeStringUTF(message);
    }

    @Override
    public void deserialize(FSTObjectInput input) throws Exception {
        this.message = input.readStringUTF();
    }
}
