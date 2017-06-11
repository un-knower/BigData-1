package somethingUtils.files;

import java.io.*;

/**
 * Created by HuShiwei on 2016/9/10 0010.
 */
public class FileUtils {

    public static void  mkdir(String parent,String child) {
        final File file = new File(parent, child);
        final boolean mkdirFlag = file.mkdir();
        if (mkdirFlag == true) {
            System.out.println("create directory successful");
        } else {
            System.out.println("create directory lose");
        }

    }

    public static void mkdirs(String parent,String child) {
        final File file = new File(parent, child);
        final boolean mkdirFlag = file.mkdirs();
        if (mkdirFlag == true) {
            System.out.println("create directories successful");
        } else {
            System.out.println("create directories lose");
        }
    }

    public static void createNewFile(String filename) {
        final File file = new File(filename);
        try {
            final boolean newFile = file.createNewFile();
            if (newFile == true) {
                System.out.println("create "+filename+" successful");
            } else {
                System.out.println("create "+filename+" lose");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static String readFile(String path) {
        BufferedReader reader = null;
        String last = "";
        File file = new File(path);
        try {
            reader = new BufferedReader(new FileReader(file));
            String temp = "";
            while ((temp=reader.readLine())!=null) {
                last = last + temp;
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return last;
    }
    public static void readLine2String(String path) {
        BufferedReader reader = null;
        File file = new File(path);
        try {
            reader = new BufferedReader(new FileReader(file));
            String temp = "";
            while ((temp=reader.readLine())!=null) {
                System.out.println(temp);
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

    }
    public static void main(String[] args) {
//        createNewFile("C:\\jusfoun\\files\\a.txt");
        readLine2String("E:\\bigdataData\\tvdata\\2012-09-20\\ars10768@20120921013000.txt");

    }
}
