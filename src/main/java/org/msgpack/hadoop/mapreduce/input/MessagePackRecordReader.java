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

package org.msgpack.hadoop.mapreduce.input;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.hadoop.io.MessagePackWritable;
import org.msgpack.value.ImmutableValue;

import java.io.IOException;

public class MessagePackRecordReader extends RecordReader<LongWritable, MessagePackWritable> {
    private static final Log LOG = LogFactory.getLog(MessagePackRecordReader.class.getName());

    private MessageUnpacker unpacker_;

    private final LongWritable key_ = new LongWritable(0);
    private final MessagePackWritable val_;

    protected long start_;
    protected long pos_;
    protected long end_;

    private FSDataInputStream fileIn_;

    public MessagePackRecordReader() {
        val_ = new MessagePackWritable();
    }

    @Override
    public void initialize(InputSplit genericSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        FileSplit split = (FileSplit)genericSplit;
        final Path file = split.getPath();
        Configuration conf = context.getConfiguration();

        // Open the file
        FileSystem fs = file.getFileSystem(conf);
        fileIn_ = fs.open(split.getPath());

        // Create streaming unpacker
        unpacker_ = MessagePack.newDefaultUnpacker(fileIn_);

        // Seek to the start of the split
        start_ = split.getStart();
        end_ = start_ + split.getLength();
        pos_ = start_;
    }

    @Override
    public float getProgress() {
        if (start_ == end_) {
            return 0.0f;
        } else {
            return Math.min(1.0f, (pos_ - start_) / (float) (end_ - start_));
        }
    }

    @Override
    public synchronized void close() throws IOException {
    }

    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
        return key_;
    }

    @Override
    public MessagePackWritable getCurrentValue() throws IOException, InterruptedException {
        return val_;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (unpacker_.hasNext()) {
            long key = fileIn_.getPos();
            ImmutableValue obj = unpacker_.unpackValue();
            LOG.info(obj.toString());
            key_.set(key);
            val_.set(obj);
            return true;
        }
        return false;
    }
}
