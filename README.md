# flume-mysql
flume采集日志存入MySQL，支持分库分表，动态加载配置文件


## 配置详解：
### resources：
	datasource：--真实db配置，datasource名称会解析为文件名称 此例中 会解析为 db、db2、db3 三个数据源  
		db.properties    
		db2.properties    
		db3.properties    
		
	sql：--不同业务对应的SQL和日志配置  
		config1.json  
		config2.json   
		config3.json   
		
		
		
### sql配置文件详解：
    //配置支持freemarker模板语言解析表名和数据源名称  
    //split模式会把分隔后的字符串数组以array为key作为模板参数，取值方式为${array[0]}，${array[1]}，${array[n]}  
    //json模式直接转换json对像作为模板参数，取值方式${jsonkey1},${jsonkey2},${jsonkey2}  
    //日表月表等时间格式使用：[时间格式]这种方式，内部会替换成当前时间例[yyyyMMdd]会被解析成20200616  
    //表名和数据源名称可以根据不同数据解析到不同的数据源和不同表，内部已经实现根据配置分库分表   
    //30|50081|127.0.0.1|http://www.baidu.com|2020-01-01 21:54:40" 以这个日志样例，这个例子中会把表名称解析为ad_click_day3020200616  
	
`
  {
  "example": "30|50081|127.0.0.1|http://www.baidu.com|2020-01-01 21:54:40",
  "dataSourceName": "lrh_db",
  "tableName": "ad_click_day${array[0]}[yyyyMMdd]",
  "logType": "split",       //日志类型为split or json；split："|"分割的字符串，json：json字符串
  "batchSize": 500,
  "columns": [
    {
      "colunmName": "s_pid",
      "jdbcType": "int",
      "index": 0,           //split模式配置index 对应数据下标
      "jsonKey": 0,         //json模式配置jsonKey 对于json中的key
      "length": 20,         //字段长度 不设置则不限制
      "nullable": false    //nullable 为false 则这个字段为null时会丢弃这条日志记录，默认为true
    },
    {
      "colunmName": "s_appid",
      "dufaultValue": "0",
      "jdbcType": "int",
      "index": 1,
      "length": 20
    },
    {
      "colunmName": "s_click_ip",
      "dufaultValue": "-",
      "jdbcType": "varchar",
      "index": 2,
      "length": 20
    },
    {
      "colunmName": "s_click_url",
      "dufaultValue": "-",
      "jdbcType": "varchar",
      "index": 3,
      "length": 200
    },
    {
      "colunmName": "d_click",
      "jdbcType": "datetime",
      "dateFormat": "yyyy-MM-dd HH:mm:ss",  //该字段为时间时对于的时间格式2020-06-16 00:00:00->yyyy-MM-dd HH:mm:ss;
											// nginx的$time_iso8601 时间格式2020-06-16T00:00:00+08:00->yyyy-MM-dd'T'HH:mm:ss+08:00
											// nginx的$msc为1592317599.569->timestamp， 常规时间戳1592317599569->timestamp
      "dufaultValue": "now()",             //默认值，如果该字段为空时使用默认值，当格式为datetime或则timestamp 并且默认值为"now()"时解析成当前时间
      "index": 4
    },
    {
      "colunmName": "d_create",
      "jdbcType": "datetime",
      "userNow": true    //userNow为true时 insert sql会把次字段值解析成 now()，可不用配置 index 或者jsonkey
    }
  ]
}
`
  
  
