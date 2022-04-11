package com.wugui.datax.admin.tool.datax.reader;

import com.google.common.collect.Maps;
import com.wugui.datax.admin.tool.pojo.DataxHivePojo;

import java.util.Map;

/**
 * hive reader 构建类
 *
 * @author jingwk
 * @version 2.0
 * @since 2020/01/05
 */
public class HiveReader extends BaseReaderPlugin implements DataxReaderInterface {
    @Override
    public String getName() {
        return "hivereader";
    }

    @Override
    public Map<String, Object> sample() {
        return null;
    }
}
