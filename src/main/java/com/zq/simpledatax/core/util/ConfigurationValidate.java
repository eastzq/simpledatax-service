package com.zq.simpledatax.core.util;

import org.apache.commons.lang.Validate;

import com.zq.simpledatax.common.util.Configuration;

/**
 * Created by jingxing on 14-9-16.
 *
 * 对配置文件做整体检查
 */
public class ConfigurationValidate {
    public static void doValidate(Configuration allConfig) {
        Validate.isTrue(allConfig!=null, "");

        coreValidate(allConfig);

        pluginValidate(allConfig);

        jobValidate(allConfig);
    }

    private static void coreValidate(Configuration allconfig) {
        return;
    }

    private static void pluginValidate(Configuration allConfig) {
        return;
    }

    private static void jobValidate(Configuration allConfig) {
        return;
    }
}
