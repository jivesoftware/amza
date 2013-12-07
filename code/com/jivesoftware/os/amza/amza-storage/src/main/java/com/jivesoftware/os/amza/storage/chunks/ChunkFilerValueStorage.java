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
package com.jivesoftware.os.amza.storage.chunks;

import com.jivesoftware.os.amza.shared.ValueStorage;

public class ChunkFilerValueStorage implements ValueStorage {

    private final ChunkFiler chunkFiler;

    public ChunkFilerValueStorage(ChunkFiler chunkFiler) {
        this.chunkFiler = chunkFiler;
    }

    @Override
    public byte[] put(byte[] value) throws Exception {
        try {
            long chunkId = chunkFiler.newChunk(value.length);
            SubFiler filer = chunkFiler.getFiler(chunkId);
            filer.setBytes(value);
            filer.flush();
            return UIO.longBytes(chunkId);
        } catch (Exception x) {
            throw new RuntimeException("Failed to save value to chuck filer. ", x);
        }
    }

    @Override
    public byte[] get(byte[] key) throws Exception {
        SubFiler filer = chunkFiler.getFiler(UIO.bytesLong(key));
        return filer.toBytes();
    }

    @Override
    public void remove(byte[] key) throws Exception {
        chunkFiler.remove(UIO.bytesLong(key));
    }

    @Override
    public void clear() {
        // TODO
        //chunkFiler.clear();
    }

}
