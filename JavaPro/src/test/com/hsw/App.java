package com.hsw;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.Test;

/**
 * Created by hushiwei on 2017/3/29.
 */
public class App {
    @org.junit.Test
    public void testlist() throws Exception {

        HashMap<String, String> map = new HashMap<>();
        map.put("app_cat", "null");

        Set<String> keySet1 = map.keySet();
        for (String s : keySet1) {
            System.out.println(s+" : "+map.get(s));
        }



        if (map.get("app_cat") == "null") {
            map.put("app_cat", "666");
        }

        System.out.println("------------------------");
        Set<String> keySet = map.keySet();
        for (String s : keySet) {
            System.out.println(s+" : "+map.get(s));
        }
    }


    @Test
    public void testmap() throws Exception {
        Map<String, String> devNames = new HashMap<String, String>();

        devNames.put("HUAWEI", "华为");
        devNames.put("GIONEE", "金立");
        devNames.put("LENOVO", "联想");
        devNames.put("MEIZU	", "魅族");
        devNames.put("VIVO	", "VIVO");
        devNames.put("COOLPAD", "酷派");
        devNames.put("HONOR	", "华为");
        devNames.put("SAMSUNG", "三星");
        devNames.put("NUBIA	", "努比亚");
        devNames.put("OPPO	", "OPPOO");
        devNames.put("YULONG", "酷派");
        devNames.put("XIAOMI", "小米");
        devNames.put("ZUK", "联想");

        String name = "hello";
        String s = devNames.get(name);

    }
}
