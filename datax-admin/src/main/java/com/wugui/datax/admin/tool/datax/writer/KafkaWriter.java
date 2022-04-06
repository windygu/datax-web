package com.wugui.datax.admin.tool.datax.writer;

import com.google.common.collect.Maps;
import com.wugui.datax.admin.tool.pojo.DataxHivePojo;
import com.wugui.datax.admin.tool.pojo.KafkaPojo;

import java.util.Map;

/**
 * mysql writer构建类
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MysqlWriter
 * @Version 1.0
 * @since 2019/7/30 23:08
 */
public class KafkaWriter extends BaseWriterPlugin implements DataxWriterInterface {
    @Override
    public String getName() {
        return "kafkawriter";
    }


    @Override
    public Map<String, Object> sample() {
        return null;
    }
    @Override
    public Map<String, Object> buildKafka(KafkaPojo plugin) {
        Map<String, Object> writerObj = Maps.newLinkedHashMap();
        writerObj.put("name", getName());

        Map<String, Object> parameterObj = Maps.newLinkedHashMap();
        parameterObj.put("brokerList", plugin.getBrokerList());
        parameterObj.put("topic", plugin.getTopic());
        parameterObj.put("partitions", plugin.getPartitions());
        parameterObj.put("batchSize", plugin.getBatchSize());
        parameterObj.put("column", plugin.getColumns());
        writerObj.put("parameter", parameterObj);
        return writerObj;
    }
}
