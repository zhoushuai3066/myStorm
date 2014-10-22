package com.zhoushuai.myStorm;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class WordCountBolt implements IRichBolt{
	
	Map<String,Integer> counters;
	private OutputCollector collector;
	Integer id;
	String name;

	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.counters = new HashMap<String, Integer>();
		this.collector=collector;
		this.name=context.getThisComponentId();
		this.id=context.getThisTaskId();
	}
	
	
	// 创建静态全局变量  
    static Connection conn;  
  
    static Statement st;  
    
    /* 获取数据库连接的函数*/  
    public static Connection getConnection() {  
        Connection con = null;  //创建用于连接数据库的Connection对象  
        try {  
            Class.forName("com.mysql.jdbc.Driver");// 加载Mysql数据驱动  
              
            con = DriverManager.getConnection(  
                    "jdbc:mysql://192.168.108.147/mycall_admin", "hadoop", "hadoop123");// 创建数据连接  
              
        } catch (Exception e) {  
            System.out.println("数据库连接失败" + e.getMessage());  
        }  
        return con; //返回所建立的数据库连接  
    }  
    
	public static void insert(String word,int value) {  
        
        conn = getConnection(); // 首先要获取连接，即连接到数据库  
  
        try {  
            String sql = "INSERT INTO stormWordCount(word,count)"  
                    + " VALUES ('"+word+"','"+value+"')";  // 插入数据的sql语句  
              
            st = (Statement) conn.createStatement();    // 创建用于执行静态sql语句的Statement对象  
              
            int count = st.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数  
              
            System.out.println("向stormWordCount表中插入 " + count + " 条数据"); //输出插入操作的处理结果  
              
        } catch (SQLException e) {  
            System.out.println("插入数据失败" + e.getMessage());  
        }  
    }  
	
public  void persistent(String word,int value) {  
        
        conn = getConnection(); // 首先要获取连接，即连接到数据库  
  
        try {  
            String sql = "SELECT * FROM stormWordCount where word = '"+word+"'";  
              
            st = (Statement) conn.createStatement();    // 创建用于执行静态sql语句的Statement对象  
              
            ResultSet rs = st.executeQuery(sql);
            if(rs.next()){
            	String count = rs.getString(2);
            	int ct = Integer.parseInt(count)+value;
            	update(word,ct);
            	collector.emit(new Values(word, ct));
            }else{
            	insert(word,value);
            	collector.emit(new Values(word, 1));
            }
              
              
        } catch (SQLException e) {  
            System.out.println("插入数据失败" + e.getMessage());  
        }  
    }  


public static void update(String word,int value) {  
    
    conn = getConnection(); // 首先要获取连接，即连接到数据库  

    try {  
        String sql = "update stormWordCount set count = '"+value+"' where word = '"+word+"'" ;
          
        st = (Statement) conn.createStatement();    // 创建用于执行静态sql语句的Statement对象  
          
        int count = st.executeUpdate(sql);  // 执行插入操作的sql语句，并返回插入数据的个数  
          
        System.out.println("向stormWordCount表中更新" + count + " 条数据"); //输出插入操作的处理结果  
          
    } catch (SQLException e) {  
        System.out.println("插入数据失败" + e.getMessage());  
    }  
}  
	

	public void execute(Tuple tuple) {
		
		 String word = tuple.getString(0);
//         Integer count = counters.get(word);
//         if(count==null) count = 0;
//         count++;
//         counters.put(word, count);
//         insert(word,count);
		 persistent(word,1);
         
         
	}

	public void cleanup() {
		 try {
			st.close();
			conn.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}  
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		  declarer.declare(new Fields("word", "count"));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}



}
