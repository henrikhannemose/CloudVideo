package master;

import java.util.ArrayList;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;

import slave.SlaveInterface.ISlave;

public class Master {
	private boolean enableJobCaching = false; //Caches job to avoid re-transcoding the same file twice
	private ArrayList<Job> completedJobs = new ArrayList<Job>();
	private long clientIDs = -1;
	private ResourcePool resourcePool;
	public boolean notTerminated = true;
	private BlockingDeque<Job> queue;
	private VMHandler vmhandler;
	
	public static void main(String[] args) {
		System.out.println("Starting master");
		Master m = new Master();
		ClientServer cserver = new ClientServer(m, 15050);
		cserver.start();
		System.out.println("Master started");
		m.runMaster();
	}

	public Master()
	{
		queue=new LinkedBlockingDeque<Job>();
		resourcePool= new ResourcePool();	
		vmhandler = new VMHandler(this,resourcePool);
		resourcePool.setVMHandler(vmhandler);
		vmhandler.start();
	}
	
	public void runMaster()
	{
		while(true)
		{
			System.out.println("Waiting for something in queue...");
			try {
				Job j = queue.take();
				System.out.println("Took job from queue: "+j);
								
				ISlave slave = resourcePool.getAvailableSlave();
				if(slave==null)
				{
					System.out.println("No slave available. Waiting until one is free.");
				}
				while(slave==null)
				{
					//System.out.println("No slave available for running job "+j+". Busy waiting");
					Thread.sleep(500);
					slave = resourcePool.getAvailableSlave();
				}
				System.out.println("Launching thread for running job "+j+" on slave: "+ slave);
				MasterJobRunnerThread runner = new MasterJobRunnerThread(j,slave, this);
				runner.start();
				System.out.println("Thread running happily doing its thing");				
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void addToPool(String host, String id)
	{
		resourcePool.addToPool(host, id);
	}
	
	public void enqueueAndRunJob(Job job) 
	{
		boolean foundMatchingJob=false;
		if(enableJobCaching)
		{
			System.out.println("Job caching enabled. Checking if similar job has already been run");
			for (Job otherJ : completedJobs) {
				foundMatchingJob = job.sameOutputFile(otherJ);
				if(foundMatchingJob)
				{
					System.out.println("Found another job with same output file!");
					System.out.println("This job= "+job);
					System.out.println("Other job="+otherJ);
					System.out.println("Notifying client to simply fetch the already existing file at: "+otherJ.getActualOutputFile());
					job.notifyClient(otherJ.getActualOutputFile());
					break;
				}
			}
		}
		if(!foundMatchingJob)
		{
			System.out.println("Putting job in queue: " + job);
			queue.offer(job);
		}
	}
			
	public void terminate() {
		notTerminated = false;
	}
	
	public boolean isNotTerminated() {
		return notTerminated;
	}
	
	public synchronized long getClientID() {
		if (clientIDs == Long.MAX_VALUE)
			clientIDs = -1;
			
		clientIDs++;
		return clientIDs;
	}
	
	public int getQueueLength() {
		return queue.size();
	}

	public boolean reRunJob(Job j) {
		return queue.offerFirst(j);
	}

	public void handleFailedSlave(String slaveInstanceID) {
		vmhandler.handleFailedSlave(slaveInstanceID);
	}

	public void logCompletedJob(Job j) {
		if(enableJobCaching)
		{
			completedJobs.add(j);
		}
	}

	public boolean getEnableJobCaching()
	{
		return enableJobCaching;
	}
}
