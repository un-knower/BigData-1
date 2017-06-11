package com.hsw.Avro;

/**
 * Created by hushiwei on 17-2-10.
 */
public class User {
    private String Name;
    private int FavoriteNumber;
    private String FavoriteColor;

    public User() {
    }

    public User(String name, int favoriteNumber, String favoriteColor) {
        Name = name;
        FavoriteNumber = favoriteNumber;
        FavoriteColor = favoriteColor;
    }

    public String getName() {
        return Name;
    }

    public void setName(String name) {
        Name = name;
    }

    public int getFavoriteNumber() {
        return FavoriteNumber;
    }

    public void setFavoriteNumber(int favoriteNumber) {
        FavoriteNumber = favoriteNumber;
    }

    public String getFavoriteColor() {
        return FavoriteColor;
    }

    public void setFavoriteColor(String favoriteColor) {
        FavoriteColor = favoriteColor;
    }
}
