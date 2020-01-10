package com.sdiread.statistic.hbase.base;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/**
 * @Description
 * @Author fyn
 * @Date 2019/4/16
 **/
public class HBaseBeanUtil {

    private static final Logger logger = LoggerFactory.getLogger(HBaseBeanUtil.class);

    private static final String INTEGER = "Integer";
    private static final String LONG = "Long";
    private static final String STRING = "String";
    private static final String HEADER_R_ID= "rId";
    private static final String HEADER_U_ID= "uId";
    private static final String ROW_KEY  = "rowkey";
    private static final String SET = "set";

    /**
     * JavaBean转换为Put
     * @param <T>
     * @param obj
     * @return
     * @throws Exception
     */
    public static <T> Put beanToPut(T obj) throws Exception {
        Put put = new Put(Bytes.toBytes(parseObjId(obj)));
        List<Field> getFields = new LinkedList<>();
        List<Field> getFieldList = HBaseBeanUtil.getFieldList(obj.getClass(),getFields);
        for (Field field : getFieldList) {
            if (!field.isAnnotationPresent(HbaseColumn.class)) {
                continue;
            }
            field.setAccessible(true);
            HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
            String family = orm.family();
            String qualifier = orm.qualifier();
            if (StringUtils.isBlank(family) || StringUtils.isBlank(qualifier)) {
                continue;
            }
            Object fieldObj = field.get(obj);
            if (Objects.nonNull(fieldObj)&&fieldObj.getClass().isArray()) {
                logger.error("..................hbase beanToPut nonsupport field:{} ",field);
            }
            if (ROW_KEY.equalsIgnoreCase(qualifier) || ROW_KEY.equalsIgnoreCase(family)) {
                continue;
            }
            if (Objects.nonNull(field)&&Objects.nonNull(field.get(obj))) {
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(field.get(obj).toString()));
            }
        }
        return put;
    }

    /**
     * 获取Bean中的id,作为Rowkey
     * @param <T>
     *
     * @param obj
     * @return
     */
    public static <T> String parseObjId(T obj) {
        Class<?> clazz = obj.getClass();
        try {
            Field field = clazz.getDeclaredField("id");
            field.setAccessible(true);
            Object object = field.get(obj);
            return object.toString();
        } catch (NoSuchFieldException e) {
            logger.error("", e);
        } catch (SecurityException e) {
            logger.error("", e);
        } catch (IllegalArgumentException e) {
            logger.error("", e);
        } catch (IllegalAccessException e) {
            logger.error("", e);
        }
        return "";
    }

    /**
     * HBase result 转换为 bean
     * @param <T>
     * @param result
     * @param obj
     * @return
     * @throws Exception
     */
    public static <T> T resultToBean(Result result, T obj) throws Exception {
        if (result == null) {
            return null;
        }
        Class<?> clazz = obj.getClass();
        List<Field> getFields = new LinkedList<>();
        List<Field> getFieldList = HBaseBeanUtil.getFieldList(clazz,getFields);
        for (Field field : getFieldList) {
            if (!field.isAnnotationPresent(HbaseColumn.class)) {
                continue;
            }
            HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
            String family = orm.family();
            String qualifier = orm.qualifier();
            boolean timeStamp = orm.timestamp();
            if (StringUtils.isBlank(family) || StringUtils.isBlank(qualifier)) {
                continue;
            }
            String fieldName = field.getName();
            Object value = "";
            if (ROW_KEY.equalsIgnoreCase(family)) {
                value = new String(result.getRow());
            } else {
                value = getResultValueByType(result, family, qualifier, timeStamp);
            }
            String firstLetter = fieldName.substring(0, 1).toUpperCase();
            if(Objects.nonNull(fieldName)&&(fieldName.equals(HEADER_R_ID)||field.getName().equals(HEADER_U_ID))){
                firstLetter = fieldName.substring(0, 1).toLowerCase();
            }
            String setMethodName = SET + firstLetter + fieldName.substring(1);
            if(field.getType().toString().contains(LONG)&&Objects.nonNull(value)){
                value = Long.valueOf(String.valueOf(value));
            }else if(field.getType().toString().contains(STRING)&&Objects.nonNull(value)){
                value = String.valueOf(value);
            }else if(field.getType().toString().contains(INTEGER)&&Objects.nonNull(value)){
                value = Integer.valueOf(String.valueOf(value));
            }
            if(Objects.nonNull(value)){
                Method setMethod = clazz.getMethod(setMethodName, new Class[] { field.getType() });
                setMethod.invoke(obj, new Object[] { value });
            }
        }
        return obj;
    }

    public static Map<String,Object> resultToMap(Result result, Object obj) throws Exception {
        Map<String,Object> retMap = new HashMap<>();
        if (result == null) {
            return null;
        }
        Class<?> clazz = obj.getClass();
        List<Field> getFields = new LinkedList<>();
        List<Field> getFieldList = HBaseBeanUtil.getFieldList(clazz,getFields);
        for (Field field : getFieldList) {
            if (!field.isAnnotationPresent(HbaseColumn.class)) {
                continue;
            }
            HbaseColumn orm = field.getAnnotation(HbaseColumn.class);
            String family = orm.family();
            String qualifier = orm.qualifier();
            boolean timeStamp = orm.timestamp();
            if (StringUtils.isBlank(family) || StringUtils.isBlank(qualifier)) {
                continue;
            }
            String fieldName = field.getName();
            Object value = "";
            if (ROW_KEY.equalsIgnoreCase(family)) {
                value = new String(result.getRow());
            } else {
                value = getResultValueByType(result, family, qualifier, timeStamp);
            }
            retMap.put(qualifier,value);

        }
        return retMap;
    }
    /**
     * @param result
     * @param family
     * @param qualifier
     * @param timeStamp
     * @return
     */
    private static String getResultValueByType(Result result, String family, String qualifier, boolean timeStamp) {
        if (!timeStamp) {
            if(Objects.nonNull(result.getValue(Bytes.toBytes(family),Bytes.toBytes(qualifier))))
                return new String(result.getValue(Bytes.toBytes(family), Bytes.toBytes(qualifier)));
            else
                return null;
        }
        List<Cell> cells = result.getColumnCells(Bytes.toBytes(family), Bytes.toBytes(qualifier));
        if (cells.size() == 1) {
            Cell cell = cells.get(0);
            return cell.getTimestamp() + "";
        }
        return "";
    }

    public static List<Field> getFieldList(Class clazz,List<Field> fieldList){
        if(null == clazz){
            return null;
        }
        Field[] fields = clazz.getDeclaredFields();
        for(Field field : fields){
            /** 过滤静态属性**/
            if(Modifier.isStatic(field.getModifiers())){
                continue;
            }
            /** 过滤transient 关键字修饰的属性**/
            if(Modifier.isTransient(field.getModifiers())){
                continue;
            }
            fieldList.add(field);
        }
        /** 处理父类字段**/
        Class<?> superClass = clazz.getSuperclass();
        if(superClass.equals(Object.class)){
            return fieldList;
        }
        List<Field> fieldListTemp =getFieldList(superClass,fieldList);
        return fieldListTemp;
    }
}
