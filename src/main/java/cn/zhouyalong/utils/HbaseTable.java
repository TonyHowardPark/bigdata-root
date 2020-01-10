package com.sdiread.statistic.hbase.base;

import java.lang.annotation.*;

/**
 * @Author fyn
 * @Descriptions: 自定义注解，用于获取table
 * @Date: @Date 2019/4/16
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE })
@Inherited
public @interface HbaseTable {
    String tableName() default "";
}
