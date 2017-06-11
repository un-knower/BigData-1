# 获取配置文件的路径

+ 方法1:
```
PropertiesUtils.class.getResourceAsStream(configPath)
```

+ 方法2:
```
IP.load(IP.class.getClassLoader().getResource("ipdb.dat").getPath());

```