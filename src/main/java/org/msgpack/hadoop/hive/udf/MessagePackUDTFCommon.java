package org.msgpack.hadoop.hive.udf;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.value.ImmutableValue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class MessagePackUDTFCommon {
    private static Log LOG = LogFactory.getLog(MessagePackUDTFCommon.class.getName());

    public static Text setText(MessagePack.PackerConfig packerConfig, Text to, ImmutableValue obj) {
        if (obj.isBooleanValue()) {
            if (obj.asBooleanValue().getBoolean()) {
                to.set("1");
            } else {
                to.set("0");
            }
            return to;

        } else if (obj.isIntegerValue()) {
            to.set(Long.toString(obj.asIntegerValue().asLong()));
            return to;

        } else if (obj.isFloatValue()) {
            to.set(Double.toString(obj.asFloatValue().toDouble()));
            return to;

        } else if (obj.isArrayValue() || obj.isMapValue()) {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            MessagePacker messagePacker = packerConfig.newPacker(out);
            try {
                messagePacker.packValue(obj);
            } catch (IOException e) {
                LOG.error(e);
            }
            to.set(out.toByteArray());
            return to;
        } else if (obj.isRawValue()) {
            to.set(obj.asRawValue().asByteArray());
            return to;

        } else {
            return null;
        }
    }
}
