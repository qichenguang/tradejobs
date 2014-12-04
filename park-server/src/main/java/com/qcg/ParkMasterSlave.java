package com.qcg;

import com.fourinone.BeanContext;

public class ParkMasterSlave
{
	public static void main(String[] args)
	{
		String[][] master = new String[][]{{"localhost","1888"},{"localhost","1889"}};
		String[][] slave = new String[][]{{"localhost","1889"},{"localhost","1888"}};
		
		String[][] server = null;
        //
        String run_as = System.getProperty("run.as");
        //
        System.out.println(run_as);
		if(run_as.equals("M"))
			server = master;
		else if(run_as.equals("S"))
			server = slave;
		System.out.println(server[0][0] + ":" + Integer.parseInt(server[0][1]));
		BeanContext.startPark(server[0][0],Integer.parseInt(server[0][1]), server);
	}
}
