package com.lzumetal.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * 类描述：
 * 创建人：liaosi
 * 创建时间：2017年12月18日
 */
public class JsonParse extends UDF {

    private static ObjectMapper objectMapper = new ObjectMapper();

    public String evaluate(String line) {
        try {
            return objectMapper.readValue(line, Rate.class).toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
