package com.sdiread.statistic.hbase.base;

import java.util.Locale;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Component;

/**
 * @Description
 * @Author fyn
 * @Date 2019/4/17
 **/
@Component
public class SpringContextUtil implements ApplicationContextAware {

    private static ApplicationContext context = null;

    private final static String PROFILE_DEV = "dev";

    private final static String PROFILE_TEST = "test";

    /* (non Javadoc)
     * @Title: setApplicationContext
     * @Description: spring获取bean工具类
     * @param applicationContext
     * @throws BeansException
     * @see org.springframework.context.ApplicationContextAware#setApplicationContext(org.springframework.context.ApplicationContext)
     */
    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.context = applicationContext;
    }

    // 传入线程中
    public static <T> T getBean(String beanName) {
        return (T) context.getBean(beanName);
    }

    public static <T> T getBean(Class<T> clazz) {
        return context.getBean(clazz);
    }

    // 国际化使用
    public static String getMessage(String key) {
        return context.getMessage(key, null, Locale.getDefault());
    }

    /// 获取当前环境
    public static String getActiveProfile() {
        String profile  = context.getEnvironment().getActiveProfiles()[0];
        return profile.equals(PROFILE_DEV)? PROFILE_TEST : profile;
//        return "prod";
    }
}
