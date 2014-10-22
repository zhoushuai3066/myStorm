package com.zhoushuai.myStorm;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.drpc.LinearDRPCTopologyBuilder;

public class DRPCApp {
	
	public static void main(String[] args) throws Exception {  
	    LinearDRPCTopologyBuilder builder  
	        =new LinearDRPCTopologyBuilder("exclamation");  
	   builder.addBolt(new ExclaimBolt(), 3);  
	   
	   
	   Config conf = new Config();
//		conf.put(Config.NIMBUS_HOST, "192.168.109.241");
		conf.setDebug(true);
		conf.setNumWorkers(3);
	   if (args != null && args.length > 0) {
		   StormSubmitter.submitTopology(args[0], conf, builder.createRemoteTopology());
//			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		}
	}  


}
