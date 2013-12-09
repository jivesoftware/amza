/*
 * IReadable.java.java
 *
 * Created on 03-12-2010 11:13:54 PM
 *
 * Copyright 2010 Jonathan Colt
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jivesoftware.os.amza.storage.filer;

import java.io.IOException;

/**
 *
 * @author Administrator
 */
public interface IReadable extends ICloseable {

    /**
     *
     * @return @throws IOException
     */
    public int read() throws IOException;

    /**
     *
     * @param b
     * @return
     * @throws IOException
     */
    public int read(byte b[]) throws IOException;

    /**
     *
     * @param b
     * @param _offset
     * @param _len
     * @return
     * @throws IOException
     */
    public int read(byte b[], int _offset, int _len) throws IOException;
}