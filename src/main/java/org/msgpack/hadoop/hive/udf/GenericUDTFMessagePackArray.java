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
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessageTypeException;
import org.msgpack.core.MessageUnpacker;
import org.msgpack.value.ImmutableValue;

import java.util.ArrayList;
import java.util.List;

@Description(name = "msgpack_array",
        value = "_FUNC_(msgpackBinary, index1, index2, ..., indexN) - parse MessagePack raw binary into a array. " +
                "All the input parameters and output column types are string.")
public class GenericUDTFMessagePackArray extends GenericUDTF {
    private static Log LOG = LogFactory.getLog(GenericUDTFMessagePackArray.class.getName());

    private static final MessagePack.UnpackerConfig unpackerConfig = new MessagePack.UnpackerConfig();
    private static final MessagePack.PackerConfig packerConfig = new MessagePack.PackerConfig();

    int numCols;    // number of output columns
    int[] indexes; // array of path expressions, each of which corresponds to a column
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
            throw new UDFArgumentException("msgpack_array() takes at least two arguments: " +
                    "the MessagePack binary a key");
        }

        if (!(args[0] instanceof StringObjectInspector)) {
            throw new UDFArgumentException("msgpack_array() takes string type for the first argument");
        }

        for (int i = 1; i < args.length; ++i) {
            if (!(args[i] instanceof PrimitiveObjectInspector)) {
                throw new UDFArgumentException("msgpack_array()'s arguments have to be int type");
            }
        }

        seenErrors = false;
        pathParsed = false;
        indexes = new int[numCols];
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
                indexes[i] = PrimitiveObjectInspectorUtils.getInt(o[i + 1], (PrimitiveObjectInspector) inputOIs[i + 1]);
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
            int len = unpacker.unpackArrayHeader();
            List<ImmutableValue> array = new ArrayList<ImmutableValue>(len);
            for (int i = 0; i < len; i++) {
                array.add(unpacker.unpackValue());
            }
            for (int i = 0; i < numCols; i++) {
                ImmutableValue obj = unpacker.unpackValue();
                int index = indexes[i];
                if (array.size() > index && index > 0) {
                    obj = array.get(indexes[i]);
                }
                if (obj == null) {
                    retVals[i] = null;
                } else {
                    retVals[i] = MessagePackUDTFCommon.setText(packerConfig, cols[i], obj);
                }
            }

            forward(retVals);
            return;

        } catch (MessageTypeException e) {
            // type error, object is not an array
            if (!seenErrors) {
                LOG.error("The input is not an array: " + e + ". Skipping such error messages in the future.");
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
        return "msgpack_array";
    }
}
