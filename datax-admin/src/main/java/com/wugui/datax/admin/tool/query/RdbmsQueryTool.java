package com.wugui.datax.admin.tool.query;

import com.wugui.datax.admin.entity.JobDatasource;

import java.sql.SQLException;

/**
 * mysql数据库使用的查询工具
 *
 * @author zhouhongfa@gz-yibo.com
 * @ClassName MySQLQueryTool
 * @Version 1.0
 * @since 2019/7/18 9:31
 */
public class RdbmsQueryTool extends BaseQueryTool implements QueryToolInterface {

    public RdbmsQueryTool(JobDatasource jobDatasource) throws SQLException {
        super(jobDatasource);
    }

}
