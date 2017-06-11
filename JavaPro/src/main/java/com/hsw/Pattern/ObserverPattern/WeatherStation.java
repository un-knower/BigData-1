package com.hsw.Pattern.ObserverPattern;

/**
 * Created by HuShiwei on 2016/10/12 0012.
 */
public class WeatherStation {
    public static void main(String[] args) {
        WeatherData weatherData = new WeatherData();
        CurrentCoditionsDisplay currentCoditionsDisplay = new CurrentCoditionsDisplay(weatherData);
        JustDisplay justDisplay = new JustDisplay(weatherData);
        weatherData.setMeasurements(80, 65, 30.4f);
        weatherData.setMeasurements(85, 99, 27.4f);
        weatherData.setMeasurements(78, 90, 36.4f);
    }
}
