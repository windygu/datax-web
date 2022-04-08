package com.wugui.datax.admin.tool.pojo;

import com.wugui.datax.admin.dto.UpsertInfo;
import com.wugui.datax.admin.entity.JobDatasource;
import lombok.Data;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 用于传参，构建json
 *
 * @author jingwk
 * @ClassName DataxMongoDBPojo
 * @Version 2.0
 * @since 2020/03/14 11:15
 */
@Data
public class KafkaPojo implements Serializable {

    /**
     * kafka列名
     */
    private List<String> column;

    /**
     * 数据源信息
     */
    private JobDatasource jdbcDatasource;

    private String brokerList;

    private String topic;

    private int partitions=0;

    private int batchSize=1000;

}