package com.wugui.datax.executor.core.config;

import com.wugui.datatx.core.executor.impl.JobSpringExecutor;
import com.wugui.datax.executor.util.SystemUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * 读取/Users/zhennan/Documents/datax-web/datax-executor/src/main/resources/application.yml的配置
 *
 * @author xuxueli 2017-04-28
 */
//bean注解不搭配configuration就不会交给ioc管理bean单例。
@Configuration
public class DataXConfig {
    private Logger logger = LoggerFactory.getLogger(DataXConfig.class);

    private static final String DEFAULT_LOG_PATH = "log/executor/jobhandler";

//    @Value注解表示，spring将有特定类处理该属性值，具体的处理为，读取属性配置文件中对应属性的值赋值给该属性，
//    属性配置文件分两种，一种是application.properties，spring启动时会自动加载，一种是自定义属性配置文件，
//    自定义配置文件通过添加@PropertySource注解加载，此注解可以同时加载多个属性配置文件，也可以加载一个文件，
//    多个属性配置文件中包括重复属性时，采用后面的属性定义（覆盖
    @Value("${datax.job.admin.addresses}")
    private String adminAddresses;

    @Value("${datax.job.executor.appname}")
    private String appName;

    @Value("${datax.job.executor.ip}")
    private String ip;

    @Value("${datax.job.executor.port}")
    private int port;

    @Value("${datax.job.accessToken}")
    private String accessToken;

    @Value("${datax.job.executor.logpath}")
    private String logPath;

    @Value("${datax.job.executor.logretentiondays}")
    private int logRetentionDays;


    @Bean
    public JobSpringExecutor JobExecutor() {
        logger.info(">>>>>>>>>>> datax-web config init.");
        JobSpringExecutor jobSpringExecutor = new JobSpringExecutor();
        jobSpringExecutor.setAdminAddresses(adminAddresses);
        jobSpringExecutor.setAppName(appName);
        jobSpringExecutor.setIp(ip);
        jobSpringExecutor.setPort(port);
        jobSpringExecutor.setAccessToken(accessToken);
        String dataXHomePath = SystemUtils.getDataXHomePath();
        if (StringUtils.isEmpty(logPath)) {
            logPath = dataXHomePath + DEFAULT_LOG_PATH;
        }
        jobSpringExecutor.setLogPath(logPath);
        jobSpringExecutor.setLogRetentionDays(logRetentionDays);

        return jobSpringExecutor;
    }

    /**
     * 针对多网卡、容器内部署等情况，可借助 "spring-cloud-commons" 提供的 "InetUtils" 组件灵活定制注册IP；
     *
     *      1、引入依赖：
     *          <dependency>
     *             <groupId>org.springframework.cloud</groupId>
     *             <artifactId>spring-cloud-commons</artifactId>
     *             <version>${version}</version>
     *         </dependency>
     *
     *      2、配置文件，或者容器启动变量
     *          spring.cloud.inetutils.preferred-networks: 'xxx.xxx.xxx.'
     *
     *      3、获取IP
     *          String ip_ = inetUtils.findFirstNonLoopbackHostInfo().getIpAddress();
     */


}