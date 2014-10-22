package com.zhoushuai.myStorm;

import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class ExclaimBolt implements IRichBolt {  
	
	private static final long serialVersionUID = 1L;
	private OutputCollector collector;


    public void declareOutputFields(OutputFieldsDeclarer declarer) {  
       declarer.declare(new Fields("id","result"));  
   }


	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector=collector;
	}

	public void execute(Tuple input) {
			String ip = input.getString(1);  
	        collector.emit(new Values(input.getValue(0), input + "--------------------hello world-----"));  
	}

	public void cleanup() {
		
	}


	public Map<String, Object> getComponentConfiguration() {
		return null;
	}  
 
}  
  

