package com.horizon.storm.kafkahbase;


import org.apache.storm.spout.Scheme;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.util.List;

/**
 * Created by admin on 2017/5/25.
 */
public class MyKHScheme implements Scheme{

    @Override
    public List<Object> deserialize(ByteBuffer byteBuffer) {
        String word=decode(byteBuffer);
        return new Values(word);
    }

    @Override
    public Fields getOutputFields() {
        return new Fields("word");
    }

    public String decode(ByteBuffer byteBuffer){
        Charset charset = null;
        CharsetDecoder decoder = null;
        CharBuffer charBuffer = null;

        charset = Charset.forName("UTF-8");
        decoder = charset.newDecoder();
        try {
            charBuffer  =  decoder.decode(byteBuffer);
        } catch (CharacterCodingException e) {
            e.printStackTrace();
        }
        return charBuffer.toString();
    }
}
