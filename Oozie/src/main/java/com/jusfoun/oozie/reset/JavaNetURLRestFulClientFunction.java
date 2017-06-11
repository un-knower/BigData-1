package com.jusfoun.oozie.reset;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

/**
 * Created by admin on 2016/4/19.
 */
public class JavaNetURLRestFulClientFunction {

    public  String RestStringToJson(String restString) throws InterruptedException {
        String json=null;
        try {
            URL restServiceURL = new URL(restString);
            HttpURLConnection httpURLConnection = (HttpURLConnection) restServiceURL.openConnection();
            httpURLConnection.setRequestMethod("GET");
            httpURLConnection.setRequestProperty("Accept","application/json");
            if (httpURLConnection.getResponseCode() != 200) {
                throw new RuntimeException("HTTP GET Request Failed with Error code" + httpURLConnection.getResponseCode());
            }
            BufferedReader responseBuffer = new BufferedReader(new InputStreamReader(httpURLConnection.getInputStream()));
            String output=null;

//            System.out.println("Output from Server: \n");

            while ((output = responseBuffer.readLine()) != null) {
                json = output;
            }

            httpURLConnection.disconnect();
        } catch (MalformedURLException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json;
    }
}
