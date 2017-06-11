package cn.scala.chapter13

import scala.beans.BeanProperty

//Student类用泛型定义，成员变量name及age指定泛型参数
//并且用注解的方式生成JavaBean规范的getter方法
//因为是val的，所以只会生成getter方法
class Student[T,S](@BeanProperty val name:T,@BeanProperty val age:S){

}
