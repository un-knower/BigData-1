package cn.Algorithm.stack;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 */
public class Examples {
    /**
     * 利用栈的先进后出特性,实现:
     * 十进制数到八进制数的转换
     * @param i
     */
    public static void baseConversion(int i) {
        StackSLinked tmp = new StackSLinked();
        while (i > 0) {
            tmp.push(i % 8 + "");
            i = i / 8;
        }
        while (!tmp.isEmpty()) {
            System.out.print((String)tmp.pop());
        }
    }

    /**
     * 判断字符串str中的括号是否正确匹配
     *
     * @param str
     * @return
     */
    public static boolean bracketMatch(String str) {
        StackSLinked tmp = new StackSLinked();
        for (int i = 0; i < str.length(); i++) {
            char c = str.charAt(i);
            switch (c) {
                case '{':
                case '[':
                case '(':
                    tmp.push(Integer.valueOf(c));
                    break;
                case '}':
                    if (!tmp.isEmpty() && ((Integer) tmp.pop()).intValue() == '{') {
                        break;
                    } else {
                        return false;
                    }
                case ']':
                    if (!tmp.isEmpty() && ((Integer) tmp.pop()).intValue() == '[') {
                        break;
                    } else {
                        return false;
                    }
                case ')':
                    if (!tmp.isEmpty() && ((Integer) tmp.pop()).intValue() == '(') {
                        break;
                    } else {
                        return false;
                    }
            }
        }
        if (tmp.isEmpty()) {
            return true;
        } else {
            return false;
        }
    }
    public static void main(String[] args) {
        baseConversion(657);
        System.out.println();
        boolean b = bracketMatch("([{88}])");
        System.out.println(b);
    }
}
