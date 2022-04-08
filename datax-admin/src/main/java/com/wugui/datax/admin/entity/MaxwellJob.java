package com.wugui.datax.admin.entity;
import io.swagger.annotations.ApiModelProperty;
import lombok.Getter;
import lombok.Setter;
import org.springframework.util.StringUtils;

@Setter
@Getter
public class MaxwellJob {
    private int id;
    @ApiModelProperty("mysql地址")
    private String mysqlHost;
    @ApiModelProperty("mysql用户")
    private String mysqlUser;
    @ApiModelProperty("mysql密码")
    private String mysqlPassword;
    @ApiModelProperty("kafka地址")
    private String kafkaServer;
    @ApiModelProperty("kafka主题")
    private String kafkaTopic;


}
