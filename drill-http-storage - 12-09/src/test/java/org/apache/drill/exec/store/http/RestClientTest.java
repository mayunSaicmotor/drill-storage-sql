package org.apache.drill.exec.store.http;


import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This example demonstrates an alternative way to call a URL
 * using the Apache HttpClient HttpGet and HttpResponse
 * classes.
 * 
 * I copied the guts of this example from the Apache HttpClient
 * ClientConnectionRelease class, and decided to leave all the
 * try/catch/finally handling in the class. You don't have to catch
 * all the exceptions individually like this, I just left the code
 * as-is to demonstrate all the possible exceptions.
 * 
 * Apache HttpClient: http://hc.apache.org/httpclient-3.x/
 *
*/
public class RestClientTest {
	
	  private static final Logger logger = LoggerFactory .getLogger(RestClientTest.class);

	public static void main(String[] args) {
		doPost();
	}
	public static String doPost()  
    {  
        String uriAPI = "http://saic-dn01:8047";//Post方式没有参数在这里  
        String result = "";  
        HttpPost httpRequst = new HttpPost(uriAPI);//创建HttpPost对象  
          
        List <NameValuePair> params = new ArrayList<NameValuePair>();  
        params.add(new BasicNameValuePair("queryType", "SQL"));  
        params.add(new BasicNameValuePair("query", "select count(*) from http.`student`"));  
          
        try {  
            httpRequst.setEntity(new UrlEncodedFormEntity(params,HTTP.UTF_8));  
            HttpResponse httpResponse = new DefaultHttpClient().execute(httpRequst);  
            if(httpResponse.getStatusLine().getStatusCode() == 200)  
            {  
                HttpEntity httpEntity = httpResponse.getEntity();  
                result = EntityUtils.toString(httpEntity);//取出应答字符串  
                
                logger.info("result: "+result);
            }  
        } catch (UnsupportedEncodingException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
            result = e.getMessage().toString();  
        }  
        catch (ClientProtocolException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
            result = e.getMessage().toString();  
        }  
        catch (IOException e) {  
            // TODO Auto-generated catch block  
            e.printStackTrace();  
            result = e.getMessage().toString();  
        }  
        return result;  
    }
	
	public static void get2(String[] args) {
	    DefaultHttpClient httpclient = new DefaultHttpClient();
	    try {
	      // specify the host, protocol, and port
	      HttpHost target = new HttpHost("10.32.47.108", 8047, "http");
	      
	      // specify the get request
	      HttpGet getRequest = new HttpGet("/forecastrss?p=80020&u=f");

	      System.out.println("executing request to " + target);

	      HttpResponse httpResponse = httpclient.execute(target, getRequest);
	      
	      HttpEntity entity = httpResponse.getEntity();

	      System.out.println("----------------------------------------");
	      System.out.println(httpResponse.getStatusLine());
	      Header[] headers = httpResponse.getAllHeaders();
	      for (int i = 0; i < headers.length; i++) {
	        System.out.println(headers[i]);
	      }
	      System.out.println("----------------------------------------");

	      if (entity != null) {
	        System.out.println(EntityUtils.toString(entity));
	      }

	    } catch (Exception e) {
	      e.printStackTrace();
	    } finally {
	      // When HttpClient instance is no longer needed,
	      // shut down the connection manager to ensure
	      // immediate deallocation of all system resources
	      httpclient.getConnectionManager().shutdown();
	    }
	  }
	
  public final static void get() {
    
    HttpClient httpClient = new DefaultHttpClient();
    try {
      // this twitter call returns json results.
      // see this page for more info: https://dev.twitter.com/docs/using-search
      // http://search.twitter.com/search.json?q=%40apple

      // Example URL 1: this yahoo weather call returns results as an rss (xml) feed
      //HttpGet httpGetRequest = new HttpGet("http://weather.yahooapis.com/forecastrss?p=80020&u=f");
      
      // Example URL 2: this twitter api call returns results in a JSON format
      HttpGet httpGetRequest = new HttpGet("http://10.32.47.108:8047/");

      // Execute HTTP request
      HttpResponse httpResponse = httpClient.execute(httpGetRequest);

      System.out.println("----------------------------------------");
      System.out.println(httpResponse.getStatusLine());
      System.out.println("----------------------------------------");

      // Get hold of the response entity
      HttpEntity entity = httpResponse.getEntity();

      // If the response does not enclose an entity, there is no need
      // to bother about connection release
      byte[] buffer = new byte[1024];
      if (entity != null) {
        InputStream inputStream = entity.getContent();
        try {
          int bytesRead = 0;
          BufferedInputStream bis = new BufferedInputStream(inputStream);
          while ((bytesRead = bis.read(buffer)) != -1) {
            String chunk = new String(buffer, 0, bytesRead);
            System.out.println(chunk);
          }
        } catch (IOException ioException) {
          // In case of an IOException the connection will be released
          // back to the connection manager automatically
          ioException.printStackTrace();
        } catch (RuntimeException runtimeException) {
          // In case of an unexpected exception you may want to abort
          // the HTTP request in order to shut down the underlying
          // connection immediately.
          httpGetRequest.abort();
          runtimeException.printStackTrace();
        } finally {
          // Closing the input stream will trigger connection release
          try {
            inputStream.close();
          } catch (Exception ignore) {
          }
        }
      }
    } catch (ClientProtocolException e) {
      // thrown by httpClient.execute(httpGetRequest)
      e.printStackTrace();
    } catch (IOException e) {
      // thrown by entity.getContent();
      e.printStackTrace();
    } finally {
      // When HttpClient instance is no longer needed,
      // shut down the connection manager to ensure
      // immediate deallocation of all system resources
      httpClient.getConnectionManager().shutdown();
    }
  }
}
