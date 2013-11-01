package client;

import java.io.IOException;

public class ClientSimulatorThread extends Thread {

	private String file;
	private String outExtension;
	private String masterIP;
	private int port;
	private long duration=-1;
	private boolean noUploadMode;
	
	public ClientSimulatorThread(String file, String outExtension, String masterIP, int port, boolean noUploadMode)
	{
		this.file=file;
		this.outExtension=outExtension;
		this.masterIP=masterIP;
		this.port=port;
		this.noUploadMode=noUploadMode;
	}
	
	public void run()
	{
		System.out.println("Starting simulated client");
		String[] args = {file, outExtension, masterIP,Integer.toString(port),Boolean.toString(noUploadMode)};
		long startTime=System.currentTimeMillis();
		try {
			Client.main(args);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		long endTime=System.currentTimeMillis();
		duration = endTime-startTime;
		System.out.println("Simulated client exited. Duration="+duration+" ms");
	}
	
	public long getDuration()
	{
		return duration;
	}
}
