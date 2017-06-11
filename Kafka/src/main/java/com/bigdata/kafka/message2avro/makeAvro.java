package com.bigdata.kafka.message2avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.File;
import java.io.IOException;

/**
 * Created by hushiwei on 17-2-8.
 */
public class makeAvro {
    public static void main(String[] args) {
        Schema schema = null;
        try {
            schema = new Schema.Parser().parse(new File("Kafka/avsc/test_schema.avsc"));
            GenericRecord payload1 = new GenericData.Record(schema);
            payload1.put("data", "hahahaah");
            payload1.put("desc", "dbevent1");
            payload1.put("id", 111);
            System.out.println("Original Message : " + payload1);
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}
