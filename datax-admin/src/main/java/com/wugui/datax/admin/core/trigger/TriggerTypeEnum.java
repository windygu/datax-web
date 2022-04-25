package com.wugui.datax.admin.core.trigger;

import com.wugui.datax.admin.core.util.I18nUtil;

/**
 * trigger type enum
 *
 * @author xuxueli 2018-09-16 04:56:41
 */
public enum TriggerTypeEnum {

//    jobconf_trigger_type_cron=Cron触发
//            jobconf_trigger_type_manual=手动触发
//    jobconf_trigger_type_parent=父任务触发
//            jobconf_trigger_type_api=API触发
//    jobconf_trigger_type_retry=失败重试触发

    MANUAL(I18nUtil.getString("jobconf_trigger_type_manual")),
    CRON(I18nUtil.getString("jobconf_trigger_type_cron")),
    RETRY(I18nUtil.getString("jobconf_trigger_type_retry")),
    PARENT(I18nUtil.getString("jobconf_trigger_type_parent")),
    API(I18nUtil.getString("jobconf_trigger_type_api"));

    private TriggerTypeEnum(String title){
        this.title = title;
    }
    private String title;
    public String getTitle() {
        return title;
    }

}
