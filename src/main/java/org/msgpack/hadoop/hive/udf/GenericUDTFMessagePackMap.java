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

package org.msgpack.hadoop.hive.udf;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageTypeException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


@Description(name = "msgpack_map",
        value = "_FUNC_(msgpackBinary, col1, col2, ..., colN) - parse MessagePack raw binary into a map. " +
                "All the input parameters and output column types are string.")
public class GenericUDTFMessagePackMap extends GenericUDTF {
    private static Log LOG = LogFactory.getLog(GenericUDTFMessagePackMap.class.getName());

    private static final MessagePack.UnpackerConfig unpackerConfig = new MessagePack.UnpackerConfig();
    private static final MessagePack.PackerConfig packerConfig = new MessagePack.PackerConfig();

    int numCols;    // number of output columns
    String[] keys; // array of path expressions, each of which corresponds to a column
    Text[] retVals; // array of returned column values
    Text[] cols;    // object pool of non-null Text, avoid creating objects all the time
    Object[] nullVals; // array of null column values
    ObjectInspector[] inputOIs; // input ObjectInspectors
    boolean pathParsed = false;
    boolean seenErrors = false;

    @Override
    public void close() throws HiveException {
    }

    @Override
    public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
        inputOIs = args;
        numCols = args.length - 1;

        if (numCols < 1) {
            throw new UDFArgumentException("msgpack_map() takes at least two arguments: " +
                    "the MessagePack binary a key");
        }

        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("msgpack_map() takes string type for the first argument");
        }

        for (int i = 1; i < args.length; ++i) {
            if (!(args[i] instanceof StringObjectInspector)) {
                throw new UDFArgumentException("msgpack_map()'s keys have to be string type");
            }
        }

        seenErrors = false;
        pathParsed = false;
        keys = new String[numCols];
        cols = new Text[numCols];
        retVals = new Text[numCols];
        nullVals = new Object[numCols];

        for (int i = 0; i < numCols; ++i) {
            cols[i] = new Text();
            //retVals[i] = cols[i];
            nullVals[i] = null;
        }

        // construct output object inspector
        ArrayList<String> fieldNames = new ArrayList<String>(numCols);
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(numCols);
        for (int i = 0; i < numCols; ++i) {
            // column name can be anything since it will be named by UDTF as clause
            fieldNames.add("c" + i);
            // all returned type will be Text
            fieldOIs.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        }
        return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
    }

    @Override
    public void process(Object[] o) throws HiveException {

        if (o[0] == null) {
            forward(nullVals);
            return;
        }
        // get the path expression for the 1st row only
        if (!pathParsed) {
            for (int i = 0; i < numCols; ++i) {
                keys[i] = ((StringObjectInspector) inputOIs[i + 1]).getPrimitiveJavaObject(o[i + 1]);
            }
            pathParsed = true;
        }

        byte[] binary = ((StringObjectInspector) inputOIs[0]).getPrimitiveWritableObject(o[0]).getBytes();
        if (binary == null) {
            forward(nullVals);
            return;
        }
        MessageUnpacker unpacker = unpackerConfig.newUnpacker(binary);
        try {
            Map<String, ImmutableValue> map = new HashMap<String, ImmutableValue>(numCols);
            int len = unpacker.unpackMapHeader();
            for (int i = 0; i < len; i++) {
                // TODO what if not a string?
                String key = unpacker.unpackString();
                ImmutableValue value = unpacker.unpackValue();
                map.put(key, value);
            }
            for (int i = 0; i < numCols; i++) {
                ImmutableValue obj = map.get(keys[i]);
                if (obj == null) {
                    retVals[i] = null;
                } else {
                    retVals[i] = MessagePackUDTFCommon.setText(packerConfig, cols[i], obj);
                }
            }
            forward(retVals);
            return;

        } catch (MessageTypeException e) {
            // type error, object is not a map
            if (!seenErrors) {
                LOG.error("The input is not a map: " + e + ". Skipping such error messages in the future.");
                seenErrors = true;
            }
            forward(nullVals);
            return;
        } catch (Exception e) {
            // parsing error, invalid MessagePack binary
            if (!seenErrors) {
                String base64 = new String(Base64.encodeBase64(binary));
                LOG.error("The input is not a valid MessagePack binary: " + base64 + ". Skipping such error messages in the future.");
                seenErrors = true;
            }
            forward(nullVals);
            return;
        } catch (Throwable e) {
            LOG.error("MessagePack parsing/evaluation exception" + e);
            forward(nullVals);
            return;
        }
    }

    @Override
    public String toString() {
        return "msgpack_map";
    }
}
