package somethingUtils.SerializeUtil;

import java.io.*;

/**
 * 对象序列化工具
 * @author admin
 *
 */
public class ObjectSerializeUtil {
	  
	    /** 
	     * @param serStr 
	     * @throws UnsupportedEncodingException 
	     * @throws IOException 
	     * @throws ClassNotFoundException 
	     * @描述 —— 将字符串反序列化成对象 
	     */  
	    @SuppressWarnings("null")
		public static Object getObjFromStr(String serStr)  
	            throws UnsupportedEncodingException, IOException,  
	            ClassNotFoundException {
	    	if(serStr==null||serStr.equals("")) return null;
	    	ByteArrayInputStream byteArrayInputStream = null;
	    	ObjectInputStream objectInputStream = null;
	    	Object result = null;
	    	try{
	    	    String redStr = java.net.URLDecoder.decode(serStr, "UTF-8");  
	 	        byteArrayInputStream = new ByteArrayInputStream(  
	 	                redStr.getBytes("ISO-8859-1"));  
	 	        objectInputStream = new ObjectInputStream(  
	 	                byteArrayInputStream);  
	 	        result = objectInputStream.readObject();  
	    	}catch(Exception e){
	    		e.printStackTrace();
	    	}finally{
	    		objectInputStream.close();  
		        byteArrayInputStream.close(); 
	    	}
	        return result;  
	    }  
	  
	    /** 
	     * @return 
	     * @throws IOException 
	     * @throws UnsupportedEncodingException 
	     * @描述 —— 将对象序列化成字符串 
	     */  
	    public static String getStrFromObj(Serializable obj) throws IOException,  
	            UnsupportedEncodingException {  
	    	if(obj==null) return null;
	    	ByteArrayOutputStream byteArrayOutputStream = null;
	    	ObjectOutputStream objectOutputStream = null;
	    	String serStr = null;
	    	try{
	    		byteArrayOutputStream = new ByteArrayOutputStream();  
		        objectOutputStream = new ObjectOutputStream(  
		                byteArrayOutputStream);  
		        objectOutputStream.writeObject(obj);  
		        serStr = byteArrayOutputStream.toString("ISO-8859-1");  
		        serStr = java.net.URLEncoder.encode(serStr, "UTF-8");  
	    	}catch(Exception e){
	    	e.printStackTrace();
	    	}finally{
	    		objectOutputStream.close();  
		        byteArrayOutputStream.close();  
	    	}
	        return serStr;  
	    }  
	}  
	  
