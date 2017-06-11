package cn;

/**
 * Created by HuShiwei on 2016/9/21 0021.
 */
public class getFilepath {
    public static void main(String[] args) {
        final String rootPath = getFilepath.class.getResource("/").getPath();
        System.out.println(rootPath);
    }
}
