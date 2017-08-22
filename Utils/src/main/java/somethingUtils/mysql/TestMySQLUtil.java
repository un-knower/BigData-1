package somethingUtils.mysql;

/**
 * Created by hushiwei on 2017/8/22.
 */
public class TestMySQLUtil {

    private static String dmpJdbcUrl = "jdbc:mysql://10.100.215.103:3306/dmp_test?useUnicode=true&characterEncoding=utf-8";
    private static String dmpUser = "root";
    private static String dmpPasswd = "xxxxxx***";


    public static void main(String[] args) {
        MySQLUtil mySQLUtil = new MySQLUtil(dmpJdbcUrl, dmpUser, dmpPasswd);
        String[][] select   = mySQLUtil.select("select status, json_data from tb_group", true);
        for (String[] strings : select) {
            System.out.println(strings[0]+" : "+strings[1]);
        }

    }
}
