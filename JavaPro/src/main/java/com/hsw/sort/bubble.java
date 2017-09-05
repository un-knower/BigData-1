package com.hsw.sort;

/**
 * Created by hushiwei on 2017/8/31.
 * desc :
 */
public class bubble {

  public static void main(String[] args) {
    int[] arr = {5,2,67,98,2,67,78};
    for (int i : arr) {
      System.out.print(i+",");
    }
    bubbleSort(arr);
    System.out.println();
    for (int i : arr) {
      System.out.print(i+",");
    }
  }

  public static void bubbleSort(int[] arr) {
    for (int i = 0; i < arr.length; i++) {
      for (int j = 1; j < arr.length-i; j++) {
        if (arr[j-1]>arr[j]) {
          int tmp = arr[j];
          arr[j] = arr[j-1];
          arr[j-1] = tmp;
        }
      }
    }

  }

}
