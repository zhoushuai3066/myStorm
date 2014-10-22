package com.zhoushuai.myStorm;

import org.apache.thrift7.TException;

import backtype.storm.generated.DRPCExecutionException;
import backtype.storm.utils.DRPCClient;

public class Main {

	
	public static void main(String[] args) throws TException, DRPCExecutionException{
		DRPCClient client = new DRPCClient("busap-master", 3772);
		DRPCClient client2 = new DRPCClient("busap-slave1", 3772);
		String result = client.execute("exclamation", "hello zhoushuai");
		String result2 = client2.execute("exclamation", "hello zhoushuai");
		
		System.out.println(result);
		System.out.println(result2);
	}
}
