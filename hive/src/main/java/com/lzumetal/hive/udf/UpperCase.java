package com.lzumetal.hive.udf;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 类描述：将单词转换为大写
 * 创建人：liaosi
 * 创建时间：2017年12月18日
 */
public class UpperCase extends UDF {

    //这是一个重载的方法，方法签名必须是public
    public String evaluate(String source) {
        if (StringUtils.isNotBlank(source)) {
            return source.toUpperCase();
        }
        return null;
    }
}
