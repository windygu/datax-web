package com.wugui.datax.admin.dto;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

/**
 * 构建hive write dto
 *
 * @author jingwk
 * @ClassName hive write dto
 * @Version 2.0
 * @since 2020/01/11 17:15
 */
@Data
public class HiveWriterDto implements Serializable {

    private String writerDefaultFS;

    private String writerFileType;

    private String writerPath;

    private String writerFileName;

    private String writeMode;

    private String writeFieldDelimiter;

    private List<String> column;
}
