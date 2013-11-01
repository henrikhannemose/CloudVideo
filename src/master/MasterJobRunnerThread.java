package master;

import java.io.EOFException;
import java.rmi.RemoteException;

import slave.SlaveInterface.ISlave;

public class MasterJobRunnerThread extends Thread {

	Job j;
	ISlave slave;
	Master master;
	
	/**
	 * Thread responsible for actually executing a job and notifying the client on completion
	 * @param j
	 * @param slave
	 * @param m
	 */
	public MasterJobRunnerThread(Job j, ISlave slave, Master m)
	{
		this.j=j;
		this.slave=slave;
		master = m;
	}
	
    public void run() 
    {
    	String slaveInstanceID="N/A"; //Cache it in case the slave crashes
		try {
			slaveInstanceID=slave.getInstanceID();
			String outputFile = slave.runJob(j.getFilePath(),j.getFileName(), j.getOutputFileExtension(), j.getClientID());
			System.out.println("Result="+(outputFile!=null?"OK":"Not OK")+" from running job on " + slave);
			if(outputFile==null)
				j.notifyClient(":(");
			else
				j.notifyClient(outputFile);
			System.out.println("Client notified");
			master.logCompletedJob(j);
		} catch (RemoteException e) {
			Throwable t = e.getCause();
			if(!(t instanceof EOFException))
			{
				System.out.println("Unhandled type of exception");
				t.printStackTrace();
			}
			System.out.println("Connection with slave "+slaveInstanceID+" lost");
			System.out.println("Asking master to handle failed slave");
			master.handleFailedSlave(slaveInstanceID);
			System.out.println("Asking master to re-run this job");
			if(!master.reRunJob(j))
			{
				System.out.println("Error in trying to re-enqueue job!");
			}
		}
    }
}
