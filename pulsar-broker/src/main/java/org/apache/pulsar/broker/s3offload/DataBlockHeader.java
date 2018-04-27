/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.broker.s3offload;

import java.io.IOException;
import java.io.InputStream;
import org.apache.bookkeeper.common.annotation.InterfaceStability.Unstable;
import org.apache.pulsar.broker.s3offload.impl.DataBlockHeaderImpl;

/**
 * The data block header in code storage for each data block
 *
 * Currently, It is in format:
 * [ magic_word -- int ][ block_len -- int ][ first_entry_id  -- long]
 *
 * with the size: 4 + 4 + 8 = 16 Bytes
 */
@Unstable
public interface DataBlockHeader {
    /**
     * Get Magic Word for data block.
     * It is a sequence of bytes used to identify the start of a block.
     */
    int getBlockMagicWord();

    /**
     * Get the length of the block in bytes, including the header.
     */
    int getBlockLength();

    /**
     * Get the message entry Id for the first message that stored in this data block.
     */
    long getFirstEntryId();

    /**
     * Get the size of this DataBlockHeader.
     */
    int getHeaderSize();

    /**
     * Get the content of the data block header as InputStream.
     * Read out in current format.
     */
    InputStream toStream() throws IOException;

    /**
     * Get the payload start offset in this block.
     * Space before this offset is for header and alignment.
     */
    static int getDataStartOffset() {
        return DataBlockHeaderImpl.getDataStartOffset();
    }
}

