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
package com.jivesoftware.os.amza.transport.tcp.replication.serialization;

import java.io.IOException;
import java.io.Serializable;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

/**
 * All tcp replication messages implement this interface. Implementors must have a public no-args constructor for serialization to work. Janky, but hopefully
 * temporary.
 */
public interface MessagePayload extends Serializable {

    void serialize(FSTObjectOutput output) throws IOException;

    void deserialize(FSTObjectInput input) throws Exception;
}
