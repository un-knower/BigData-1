package com.hsw.Avro;

/**
 * Created by hushiwei on 17-2-10.
 */
public class makrAvroFile {
    public static void main(String[] args) {
        User user1 = new User();
        user1.setName("Alyssa");
        user1.setFavoriteNumber(256);
        User user2 = new User("Ben", 7, "red");
    }
}
