/*
 * MessagePack-Hadoop Integration
 *
 * Copyright (C) 2009-2011 MessagePack Project
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package org.msgpack.hadoop.io;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.BytesWritable;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ImmutableValue;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * A Hadoop Writable wrapper for MessagePack (untyped).
 */
public class MessagePackWritable extends BytesWritable {
    private static final Log LOG = LogFactory.getLog(MessagePackWritable.class.getName());

    protected ImmutableValue obj_ = null;
    protected byte[] bytes_;

    private ByteArrayOutputStream outStream;
    private MessagePacker packer;

    public MessagePackWritable() {}

    public MessagePackWritable(ImmutableValue obj) {
        obj_ = obj;
        initWritable(obj);
    }

    private void initWritable(ImmutableValue obj) {
        if (outStream == null) {
            outStream = new ByteArrayOutputStream();
        }
        if (packer == null) {
            packer = MessagePack.newDefaultPacker(outStream);
        }
        outStream.reset();
        try {
            packer.packValue(obj);
        } catch (IOException e) {
            LOG.error(e);
        }
        bytes_ = outStream.toByteArray();
        set(bytes_, 0, bytes_.length);
    }

    public void set(ImmutableValue obj) {
        obj_ = obj;
        initWritable(obj);
    }

    @Override
    public byte[] getBytes() {
        return bytes_;
    }

    public void write(DataOutput out) throws IOException {
        assert(obj_ != null);
        byte[] raw = getBytes();
        if (raw == null) return;
        out.writeInt(raw.length);
        out.write(raw, 0, raw.length);
    }

    @SuppressWarnings("unchecked")
    public void readFields(DataInput in) throws IOException {
        int size = in.readInt();
        if (size > 0) {
            byte[] raw = new byte[size];
            in.readFully(raw, 0, size);
            obj_ = MessagePack.newDefaultUnpacker(raw).unpackValue();
            assert(obj_ != null);
        }
    }
}
