package cn.Algorithm.recursion;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 */
public class examples {
    public static int factorial(int n) {
        if (n == 0) {
            return 1;
        } else {
            return n * factorial(n - 1);
        }
    }

    /**
     * 整数x,非负整数n
     *
     * @return x^n
     */
    public int power(int x, int n) {
        int y = 0;
        if (n == 0) {
            y = 1;
        } else {
            y = power(x, n / 2);
            y = y * y;
            if (n % 2 == 1) {
                y = y * x;
            }
        }
        return y;
    }

    /**
     * n阶Hanoi塔(汉诺塔)的移动步骤
     * @param n
     * @param x
     * @param y
     * @param z
     */
    public static void hanio(int n, char x, char y, char z) {
        if (n == 1) {
            move(x, n, z);
        } else {
            hanio(n - 1, x, z, y);
            move(x, n, z);
            hanio(n - 1, y, x, z);
        }
    }

    private static void move(char x, int n, char y) {
        System.out.println("Move " + n + " from " + x + " to " + y);
    }

    /**
     * 输出 n 个布尔量的所有可能组合。
     * @param b
     * @param n
     */
    public static void coding(int[] b, int n) {
        if (n == 0) {
            b[n]=0;outBn(b);
            b[n]=1;outBn(b);
        }else {
            b[n]=0;coding(b,n-1);
            b[n]=1;coding(b,n-1);
        }
    }
    private static void outBn(int[] b) {
        for (int i = 0; i < b.length; i++) {
            System.out.print(b[i]);
        }
        System.out.println();
    }

    public static void main(String[] args) {
        hanio(4,'x','y','z');
    }
}
