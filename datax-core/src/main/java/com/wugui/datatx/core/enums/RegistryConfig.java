package com.wugui.datatx.core.enums;

/**
 * Created by xuxueli on 17/5/10.
 */
public class RegistryConfig {

    public static final int BEAT_TIMEOUT = 30; //30s
    public static final int DEAD_TIMEOUT = BEAT_TIMEOUT * 3; //90s dead

    public enum RegistType{ EXECUTOR, ADMIN }

}
