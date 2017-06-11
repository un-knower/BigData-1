package somethingUtils.httpclient;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpException;
import org.apache.commons.httpclient.MultiThreadedHttpConnectionManager;
import org.apache.commons.httpclient.methods.*;
import org.apache.commons.httpclient.methods.multipart.MultipartRequestEntity;
import org.apache.commons.httpclient.methods.multipart.Part;
import org.apache.commons.httpclient.methods.multipart.StringPart;
import org.apache.commons.httpclient.params.HttpMethodParams;
import org.apache.http.client.params.ClientPNames;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 
 * @ClassName: HttpClientUtil 
 * @Description: HttpClientUtil 
 * @author: guozhifengvip@163.com
 * @date: 2016年3月29日 下午5:43:04
 */


public class HttpClientUtil {
	/**
	 * 
	 * @Title: BasicGetMethod 
	 * @Description: Basic 认证的get请求
	 * @param url  请求的url
	 * @param username basic 认证的用户名
	 * @param password basic 认证的密码
	 * @return
	 * @return: Map<String,String>
	 */
	public Map<String, String> BasicGetMethod(String url, String username, String password) {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);

		Map<String, String> resMap = new HashMap<String, String>();
		Base64 b = new Base64();
		String encoding = b.encodeAsString(new String(username + ":" + password).getBytes());
		client.getParams().setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, false);
		GetMethod get = new GetMethod(url);
		get.addRequestHeader("Authorization", "Basic " + encoding);
		
		get.setRequestHeader("Accept","application/json");
		get.setDoAuthentication(true);
		try {

			client.executeMethod(get);
			resMap.put("result", get.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(get.getStatusCode()));
		
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

			get.releaseConnection();
		}
		return resMap;
	}

	/**
	 * 
	 * @Title: GetMethod 
	 * @Description: 普通的get请求
	 * @param url    请求的url
	 * @return: Map<String,String>
	 */
	public Map<String, String> GetMethod(String url) {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);

		Map<String, String> resMap = new HashMap<String, String>();

		GetMethod get = new GetMethod(url);
	

		try {

			client.executeMethod(get);
			resMap.put("result", get.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(get.getStatusCode()));
		
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

			get.releaseConnection();
		}
		return resMap;
	}

	
	
  
     /**
      * 
      * @Title: PostMethod 
      * @Description: 普通的提交json的post请求
      * @param url  请求的url
      * @param transJson   post提交的json 字符串
      * @return
      * @throws HttpException
      * @throws IOException
      * @return: Map<String,String>
      */
	public  Map<String, String> PostMethod(String url, String transJson) throws HttpException, IOException {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);
		Map<String, String> resMap = new HashMap<String, String>();
		PostMethod method = null;
		try {
                     
			method = new PostMethod(url);
			RequestEntity se = new StringRequestEntity(transJson, "application/json", "UTF-8");
			method.setRequestEntity(se);
			// 使用系统提供的默认的恢复策略
           // System.out.println(transJson);
			//method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

			// 设置超时的时间

			//method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);
			client.executeMethod(method);
			resMap.put("result", method.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(method.getStatusCode()));
		} catch (IllegalArgumentException e) {

		} catch (java.io.UnsupportedEncodingException e) {

			e.printStackTrace();
		}finally {

			method.releaseConnection();
		}

		return resMap;
	}
     

    /**
     * 
     * @Title: BasicPutMethod 
     * @Description: basic 认证的put请求
     * @param url   请求的url
     * @param username   basic 认证的用户名
     * @param password     basic 认证的密码
     * @param transJson     put 提交的参数
     * @return
     * @throws HttpException
     * @throws IOException
     * @return: Map<String,String>
     */
	public  Map<String, String> BasicPutMethod(String url,String username,String password, String transJson) throws HttpException, IOException {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);
		Map<String, String> resMap = new HashMap<String, String>();
		PutMethod method = null;
		try {
          method = new PutMethod(url);
          Base64 b = new Base64();
  		String encoding = b.encodeAsString(new String(username + ":" + password).getBytes());
  		client.getParams().setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, false);
  		method.addRequestHeader("Authorization", "Basic " + encoding);
  		method.addRequestHeader("X-Requested-By","ambari");
  		method.setDoAuthentication(true);
		  RequestEntity se = new StringRequestEntity(transJson, "text/plain", "UTF-8");
			method.setRequestEntity(se);
			
			// 使用系统提供的默认的恢复策略
           // System.out.println(transJson);
			//method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

			// 设置超时的时间

			//method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);
			client.executeMethod(method);
			resMap.put("result", method.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(method.getStatusCode()));
		} catch (IllegalArgumentException e) {

		} catch (java.io.UnsupportedEncodingException e) {

			e.printStackTrace();
		}finally {

			method.releaseConnection();
		}

		return resMap;
	}
	
	/**
	 * 
	 * @Title: MultpostMethod 
	 * @Description: 通过数据流请求的post请求
	 * @param url
	 * @param para
	 * @return
	 * @throws HttpException
	 * @throws IOException
	 * @return: Map<String,String> 两个 key eg.paraName("Data") para("json串")
	 */
	public  Map<String, String> MultpostMethod(String url, Map<String,String> para,int timeout) throws HttpException, IOException {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);
		Map<String, String> resMap = new HashMap<String, String>();
		PostMethod method = null;
		try {
          method = new PostMethod(url);
  		Part[] parts = {
			      new StringPart(para.get("paraName"), para.get("para")),
			     
			  };
		 MultipartRequestEntity mrp= new MultipartRequestEntity(parts, method
               .getParams());

         
		 	method.setRequestEntity(mrp); 
		 	method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);
			client.executeMethod(method);
			resMap.put("result", method.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(method.getStatusCode()));
		} catch (IllegalArgumentException e) {

		} catch (java.io.UnsupportedEncodingException e) {

			e.printStackTrace();
		}finally {

			method.releaseConnection();
		}

		return resMap;
	}
	
	
	/**
     * 
     * @Title: BasicPutMethod 
     * @Description: basic 认证的put请求
     * @param url
     * @param username   用户名
     * @param password    密码
     * @param transJson    提交的json
     * @return
     * @throws HttpException
     * @throws IOException
     * @return: Map<String,String>
     */
	public  Map<String, String> asicPostMethod(String url,String username,String password, String transJson) throws HttpException, IOException {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);
		Map<String, String> resMap = new HashMap<String, String>();
		PostMethod method = null;
		try {
          method = new PostMethod(url);
          Base64 b = new Base64();
  		String encoding = b.encodeAsString(new String(username + ":" + password).getBytes());
  		client.getParams().setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, false);
  		method.addRequestHeader("Authorization", "Basic " + encoding);
  		method.setDoAuthentication(true);
		  RequestEntity se = new StringRequestEntity(transJson, "application/json", "UTF-8");
			method.setRequestEntity(se);
			
			// 使用系统提供的默认的恢复策略
           // System.out.println(transJson);
			//method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

			// 设置超时的时间

			//method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);
			client.executeMethod(method);
			resMap.put("result", method.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(method.getStatusCode()));
		} catch (IllegalArgumentException e) {

		} catch (java.io.UnsupportedEncodingException e) {

			e.printStackTrace();
		}finally {

			method.releaseConnection();
		}

		return resMap;
	}
    /**
     * 
     * @Title: BasicPostMethod 
     * @Description: TODO
     * @param url
     * @param username
     * @param password
     * @param transJson
     * @return
     * @return: Map<String,String>
     */
	public Map<String, String> BasicPostMethod(String url, String username, String password, String transJson) {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);
		Map<String, String> resMap = new HashMap<String, String>();
		PostMethod method = null;
		try {
          method = new PostMethod(url);
          Base64 b = new Base64();
  		String encoding = b.encodeAsString(new String(username + ":" + password).getBytes());
  		client.getParams().setParameter(ClientPNames.ALLOW_CIRCULAR_REDIRECTS, false);
  		method.addRequestHeader("Authorization", "Basic " + encoding);
  		method.setRequestHeader("Accept","application/json");
  		method.addRequestHeader("Content-Type", "application/json");
  		method.setDoAuthentication(true);
		  RequestEntity se = new StringRequestEntity(transJson, "text/plain", "UTF-8");
			method.setRequestEntity(se);
			
			// 使用系统提供的默认的恢复策略
           // System.out.println(transJson);
			//method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

			// 设置超时的时间

			//method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);
			client.executeMethod(method);
			resMap.put("result", method.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(method.getStatusCode()));
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (java.io.UnsupportedEncodingException e) {

			e.printStackTrace();
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally {

			method.releaseConnection();
		}

		return resMap;
	}
	
	
	
	
	/**
     * 
     * @Title: PostMethod 
     * @Description: 普通的提交json的post请求
     * @param url  请求的url
     * @param transJson   post提交的json 字符串
     * @return
     * @throws HttpException
     * @throws IOException
     * @return: Map<String,String>
     */
	public  Map<String, String> PostMethodCookie(String url, String transJson,List<String> cookieList) throws HttpException, IOException {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);
		Map<String, String> resMap = new HashMap<String, String>();
		PostMethod method = null;
		try {
                    
			method = new PostMethod(url);
			RequestEntity se = new StringRequestEntity(transJson, "application/json", "UTF-8");
			method.setRequestEntity(se);
			// 使用系统提供的默认的恢复策略
          // System.out.println(transJson);
			//method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER, new DefaultHttpMethodRetryHandler());

			// 设置超时的时间

			//method.getParams().setParameter(HttpMethodParams.SO_TIMEOUT, timeout);
	        if(cookieList.size()>0){
	               StringBuffer sb = new StringBuffer();
	               for (String cookie: cookieList) {
	                     int pos = cookie.indexOf(";");   
	                     sb.append(cookie.substring(0, pos));
	                     sb.append(";");
	              }
	               method.addRequestHeader("Cookie",sb.toString());
	        }
			client.executeMethod(method);
			resMap.put("result", method.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(method.getStatusCode()));
		} catch (IllegalArgumentException e) {

		} catch (java.io.UnsupportedEncodingException e) {

			e.printStackTrace();
		}finally {

			method.releaseConnection();
		}

		return resMap;
	}
	
	/**
	 * 
	 * @Title: GetMethod 
	 * @Description: 普通的get请求
	 * @param url    请求的url
	 * @return: Map<String,String>
	 */
	public Map<String, String> GetMethodCookie(String url,List<String> cookieList) {
		MultiThreadedHttpConnectionManager connectionManager = new MultiThreadedHttpConnectionManager();
		HttpClient client = new HttpClient(connectionManager);

		Map<String, String> resMap = new HashMap<String, String>();

		GetMethod get = new GetMethod(url);
	

		try {
			 if(cookieList.size()>0){
	               StringBuffer sb = new StringBuffer();
	               for (String cookie: cookieList) {
	                     int pos = cookie.indexOf(";");   
	                     sb.append(cookie.substring(0, pos));
	                     sb.append(";");
	              }
	        get.addRequestHeader("Cookie",sb.toString());
			 }
	        client.executeMethod(get);
			resMap.put("result", get.getResponseBodyAsString());
			resMap.put("resultCode", String.valueOf(get.getStatusCode()));
		
		} catch (HttpException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {

			get.releaseConnection();
		}
		return resMap;
	}
	
	
	}
	

