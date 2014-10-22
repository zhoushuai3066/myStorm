package com.zhoushuai.myStorm;



import java.util.ArrayList;


import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * Hello world!
 *
 */
public class App 
{
	
    public static void main( String[] args ) throws AlreadyAliveException, InvalidTopologyException, InterruptedException
    {
    	BrokerHosts brokerHosts = new ZkHosts("192.168.109.241");
		SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "test",
				"/storm", "word");

		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

		spoutConf.zkServers = new ArrayList<String>() {
			{
				add("192.168.109.241");
				add("192.168.109.214");
				add("192.168.109.215");
			}
		};
		spoutConf.zkPort = 2181;
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("word-KafkaSpout", new KafkaSpout(spoutConf), 1);
		builder.setBolt("word-SplitSentenceBolt", new SplitSentenceBolt(),3).shuffleGrouping("word-KafkaSpout");
		builder.setBolt("word-WordCountBolt", new WordCountBolt(), 1).fieldsGrouping("word-SplitSentenceBolt", new Fields("word"));

		Config conf = new Config();
//		conf.put(Config.NIMBUS_HOST, "192.168.109.241");
		conf.setDebug(true);
		conf.setNumWorkers(3);
		if (args != null && args.length > 0) {
			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		}

    }
}
