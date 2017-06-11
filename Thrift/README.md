### 环境准备
1. thrift生成代码
创建Thrift文件demoHello.thrift ,内容如下：
```
namespace java com.hushiwei.thrift.demo

service  HelloWorldService {
  string sayHello(1:string username)
}
```

2. thrift-0.8.0.exe 是官网提供的windows下编译工具，运用这个工具生成相关代码：

1
thrift-0.8.0.exe -r -gen java ./demoHello.thrift
生成后的目录结构如下：
G:.
│  demoHello.thrift
│  demouser.thrift
│  thrift-0.8.0.exe
│
└─gen-java
    └─com
        └─micmiu
            └─thrift
                └─demo
                        HelloWorldService.java

将生成的HelloWorldService.java 文件copy到自己测试的工程中，我的工程是用maven构建的，故在pom.xml中增加如下内容：
```
<dependency>
	<groupId>org.apache.thrift</groupId>
	<artifactId>libthrift</artifactId>
	<version>0.8.0</version>
</dependency>
<dependency>
	<groupId>org.slf4j</groupId>
	<artifactId>slf4j-log4j12</artifactId>
	<version>1.5.8</version>
</dependency>
```

### 开始写代码
1.实现接口Iface
java代码：HelloWorldImpl.java
2.TSimpleServer服务端
简单的单线程服务模型，一般用于测试。
编写服务端server代码：HelloServerDemo.java
编写客户端Client代码：HelloClientDemo.java
3.运行
先运行服务端程序，日志如下：
    HelloWorld TSimpleServer start ....
再运行客户端调用程序，日志如下：
    Thrift client result =: Hi,Hushiwei welcome to my world
测试成功，和预期的返回信息一致。

(更详细内容,看这篇文章)[http://www.micmiu.com/soa/rpc/thrift-sample/]