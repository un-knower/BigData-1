package com.jusfoun.bigdata.util.es;
import java.io.IOException;  
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.TimeZone;

import org.apache.log4j.Logger;

import com.fasterxml.jackson.annotation.JsonInclude.Include;  
import com.fasterxml.jackson.core.JsonGenerationException;  
import com.fasterxml.jackson.core.JsonParseException;  
import com.fasterxml.jackson.core.type.TypeReference;  
import com.fasterxml.jackson.databind.DeserializationFeature;  
import com.fasterxml.jackson.databind.JsonMappingException;  
import com.fasterxml.jackson.databind.ObjectMapper;  
import com.fasterxml.jackson.databind.SerializationFeature;  

public class JsonUtils {  
	
    private static Logger logger = Logger.getLogger(JsonUtils.class);  
    private static final ObjectMapper objectMapper;  
    static {  
        objectMapper = new ObjectMapper();  
        //去掉默认的时间戳格式  
        objectMapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);  
        //设置为中国上海时区  
        objectMapper.setTimeZone(TimeZone.getTimeZone("GMT+8"));  
        objectMapper.configure(SerializationFeature.WRITE_NULL_MAP_VALUES, false);  
        //空值不序列化  
        objectMapper.setSerializationInclusion(Include.NON_NULL);  
        //反序列化时，属性不存在的兼容处理  
        objectMapper.getDeserializationConfig().withoutFeatures(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);  
        //序列化时，日期的统一格式  
        objectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));  
  
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);  
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);  
        //单引号处理  
        objectMapper.configure(com.fasterxml.jackson.core.JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);  
    }  
  
    public static <T> T toObject(String json, Class<T> clazz) {  
        try {  
            return objectMapper.readValue(json, clazz);  
        } catch (JsonParseException e) {  
            logger.error(e.getMessage(), e);  
        } catch (JsonMappingException e) {  
            logger.error(e.getMessage(), e);  
        } catch (IOException e) {  
            logger.error(e.getMessage(), e);  
        }  
        return null;  
    }  
    /**
     * 字符串转化为map
     * @param str
     * @return
     */
    public static Map<String,Object> strToMap(String str){
        try {
			return objectMapper.readValue( str, Map.class);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);  
		} 
        return null ;
    }
    
    public static String toStr(Object obj) {  
        try {  
            return objectMapper.writeValueAsString(obj) ;  
        } catch (JsonParseException e) {  
            logger.error(e.getMessage(), e);  
        } catch (JsonMappingException e) {  
            logger.error(e.getMessage(), e);  
        } catch (IOException e) {  
            logger.error(e.getMessage(), e);  
        }  
        return null;  
    }  
    
    public static <T> String toJson(T entity) {  
        try {  
            return objectMapper.writeValueAsString(entity);  
        } catch (JsonGenerationException e) {  
            logger.error(e.getMessage(), e);  
        } catch (JsonMappingException e) {  
            logger.error(e.getMessage(), e);  
        } catch (IOException e) {  
            logger.error(e.getMessage(), e);  
        }  
        return null;  
    }  
 
    public static   byte[] toBytes(Object obj) {  
        try {  
            return objectMapper.writeValueAsBytes(obj ) ;  
        } catch (JsonGenerationException e) {  
            logger.error(e.getMessage(), e);  
        } catch (JsonMappingException e) {  
            logger.error(e.getMessage(), e);  
        } catch (IOException e) {  
            logger.error(e.getMessage(), e);  
        }  
        return null;  
    } 
    
    public static <T> T toCollection(String json, TypeReference<T> typeReference) {  
        try {  
            return objectMapper.readValue(json, typeReference);  
        } catch (JsonParseException e) {  
            logger.error(e.getMessage(), e);  
        } catch (JsonMappingException e) {  
            logger.error(e.getMessage(), e);  
        } catch (IOException e) {  
            logger.error(e.getMessage(), e);  
        }  
        return null;  
    }  
  
}  