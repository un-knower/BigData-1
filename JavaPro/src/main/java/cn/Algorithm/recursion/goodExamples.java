package cn.Algorithm.recursion;

/**
 * Created by HuShiwei on 2016/10/18 0018.
 */
public class goodExamples {
    /**
     * 简单的比较,返回数组中的最大最小值
     * @param arr
     * @return
     */
    public static IntPair simpleMinMax(int[] arr) {
        IntPair pair = new IntPair();
        pair.min=arr[0];
        pair.max=arr[0];
        for (int i = 0; i < arr.length; i++) {
            if (pair.min>arr[i]) {
                pair.min = arr[i];
            }
            if (pair.max < arr[i]) {
                pair.max = arr[i];
            }
        }
        return pair;
    }

    /**
     * 使用递归分治法,高效的返回出数组中的最大最小值
     * @param arr
     * @param low
     * @param high
     * @return
     */
    public static IntPair min_max(int[] arr, int low, int high) {
        IntPair pair = new IntPair();
        if (low > high - 2) {
            if (arr[low] < arr[high]) {
                pair.max = arr[high];
                pair.min = arr[low];
            } else {
                pair.min = arr[high];
                pair.max = arr[low];
            }
        } else {
            int mid = (low + high) / 2;
            IntPair p1 = min_max(arr, low, mid);
            IntPair p2 = min_max(arr, mid + 1, high);
            pair.max = p1.max > p2.max ? p1.max : p2.max;
            pair.min = p1.min < p2.min ? p1.min : p1.min;
        }

        return pair;
    }

    public static void main(String[] args) {
        int[] arr = {1,5,7,435,66,3225,668,3,56,345,5658,325,45767,333333,678,325};
//        IntPair intPair = simpleMinMax(arr);
        IntPair intPair = min_max(arr,0,arr.length-1);
        System.out.println("max: " + intPair.max + " min: " + intPair.min);
    }
}
