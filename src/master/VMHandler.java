package master;

import global.GlobalVariables;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import slave.SlaveInterface.ISlave;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesRequest;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.RunInstancesResult;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.TerminateInstancesRequest;

public class VMHandler extends Thread {
	private Properties prop = null;
	
	private boolean enableLaunchingVMs = true;
	private String tagName="cloudvideo"; //tag name used to identify all VMs belonging to us
	private String ami = "ami-f6c02081"; 
	private String instanceType = "t1.micro";
	private String securityGroup ="CloudVideo";
	private String keyName = "CloudVideo";
	
	private ClasspathPropertiesFileCredentialsProvider cred;
	private AmazonEC2Client ec2;
	private ResourcePool resourcePool = null;
	private Master master;
	private Set<String> pendingVMs = null;
	private int minimumslaves = 1;
	private int maximumslaves = 1;
	private int policy = 0;
	private boolean isStartup = true;
	
	/**
	 * A VMHandler is responsible for starting and terminating VMs
	 * @param master
	 * @param resourcePool
	 */
	public VMHandler(Master master, ResourcePool resourcePool) {
		this.master=master;
		this.resourcePool=resourcePool;
		prop = GlobalVariables.getGlobalVariables();
		pendingVMs = new HashSet<String>();
		cred = new ClasspathPropertiesFileCredentialsProvider(); //TODO: Place this globally somewhere (also used in Slave)
		ec2 = new AmazonEC2Client(cred);
		ec2.setRegion(Region.getRegion(Regions.EU_WEST_1)); //TODO: Place this globally somewhere (also used in Slave)
		policy = Integer.valueOf(prop.getProperty("elasticpolicy"));
		isStartup = true;
		getVMs();
	}

	/**
	 * Fetch any already running slaves from EC2
	 */
	private void getVMs() {
		DescribeInstancesRequest instanceReq = new DescribeInstancesRequest();
		List<String> keyNames = new ArrayList<String>();
		keyNames.add(tagName);
		Filter f = new Filter("tag-key",keyNames);
		List<String> stateNames = new ArrayList<String>();
		stateNames.add("pending");
		stateNames.add("running");
		Filter f1 = new Filter("instance-state-name",stateNames);
		List<Filter> filters = new ArrayList<Filter>();
		filters.add(f);
		filters.add(f1);
		instanceReq.withFilters(filters);
		DescribeInstancesResult instances = ec2.describeInstances(instanceReq);
		
		for (Reservation r : instances.getReservations()) {
			for (Instance i : r.getInstances()) {
				String host = i.getPublicIpAddress();
				String id = i.getInstanceId();
				if(host != null && checkSlave(host)) {
					resourcePool.addToPool(host, id);
				} else {
					pendingVMs.add(id);
				}
			}
		}
		
		minimumslaves = Integer.valueOf(prop.getProperty("minimumslaves"));
		maximumslaves = Integer.valueOf(prop.getProperty("maximumslaves"));
		// Set minimum slaves running
		int numberofslaves = resourcePool.size();
		int pending = pendingVMs.size();
		if (numberofslaves+ pending < minimumslaves) {
			launchVMs(minimumslaves - (numberofslaves + pending));
		}

		startLog();
		numberofslaves = resourcePool.size();
		pending = pendingVMs.size();
		System.out.println("VMHandler started with " + numberofslaves  + " runningslaves and " + pending + " pendingslaves.");
	}

	/**
	 * Launches the requested amount of VMs on EC2
	 * @param count
	 */
	private void launchVMs(int count)
	{
		if(enableLaunchingVMs)
		{
			System.out.println("Launching VM");
			RunInstancesRequest req = new RunInstancesRequest();
			req.withImageId(ami)
			.withInstanceType(instanceType)
			.withMinCount(count)
			.withMaxCount(count)
			.withSecurityGroups(securityGroup)
			.withKeyName(keyName);
			
			RunInstancesResult res = null;
			try {
				res = ec2.runInstances(req);
				Thread.sleep(500);
			} catch (AmazonServiceException | InterruptedException e2) {
				res = null;
				System.err.println("Amazon doesn't want to give us instances, stupid amazon :(" +
						"\nWe are busy waiting right now");
			}
			
			if (res == null) {
				return;
			}
			
			for (Instance i : res.getReservation().getInstances()) 
			{
				String id = i.getInstanceId();
				CreateTagsRequest tagsReq = new CreateTagsRequest();
				tagsReq.withResources(id);
				List<Tag> tags = new ArrayList<Tag>();
				tags.add(new Tag(tagName,tagName));
				String name = "CloudVideoSlave-" + id;
				tags.add(new Tag("Name", name));
				tagsReq.withTags(tags);
				while (true) {
					try {
						ec2.createTags(tagsReq);
						break;
					} catch (Exception e) {
						try {
							Thread.sleep(50);
						} catch (Exception e2) {
							e.printStackTrace();
						}
						continue;
					}
				}
				pendingVMs.add(id);
				System.out.println("VM launched successfully");
			}
		}
	}
	
	/**
	 * Returns whether it is possible to connect to the slave at the given host
	 * @param host
	 * @return
	 */
	public boolean checkSlave(String host) {
        try {
        	Registry registry = LocateRegistry.getRegistry(host);
			ISlave slave = (ISlave) registry.lookup("ISlave");
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
	/**
	 * Removes a VM from the system by terminating it
	 * @param id
	 */
	private void shutdownVM(String id)
	{
		resourcePool.removeSlave(id);
		TerminateInstancesRequest terminateReq = new TerminateInstancesRequest();
		Collection<String> dogtags = new HashSet<String>();
		dogtags.add(id);
		terminateReq.setInstanceIds(dogtags);
		ec2.terminateInstances(terminateReq);
		System.out.println("VM "+id+" terminated successfully");
	}
	
	/**
	 * Shuts down one available VM
	 */
	public void shutdownVM() {
		try {
			ISlave slave = null;
			for (int i = 0; slave == null && i < 100; i++) {
				slave = resourcePool.getAvailableSlave();
				if (slave == null) {
					Thread.sleep(10);
				}
			}
			if (slave == null)
				return;
			shutdownVM(slave.getInstanceID());
		} catch (RemoteException | InterruptedException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Checks whether pending VMs (booting VMs) have moved to the running state
	 */
	public void checkPendingVMS() {
		if (pendingVMs.size() == 0) {
			return;
		}
		DescribeInstancesRequest instanceReq = new DescribeInstancesRequest();
		List<String> keyNames = new ArrayList<String>();
		keyNames.add(tagName);
		Filter f = new Filter("tag-key",keyNames);
		List<Filter> filters = new ArrayList<Filter>();
		filters.add(f);
		instanceReq.withFilters(filters);
		DescribeInstancesResult instances = ec2.describeInstances(instanceReq);
		
		for (Reservation r : instances.getReservations()) {
			for (Instance i : r.getInstances()) {
				String host = i.getPublicIpAddress();
				String id = i.getInstanceId();
				if (pendingVMs.contains(id)
						&& host != null
						&& i.getState().getName().toString().equals("running")
						&& checkSlave(host)) {
					System.out.println("pending VM is running, adding to the pool :D");
					moveSlaveFromPendingToRunning(host,id);
				}
			}
		}
	}
	
	public void run() {
		startLog();
		
		int counter = 0;
		int[] slidingwindow = new int[5];
		while(true) {
			int jobs = master.getQueueLength();
			int slaves = resourcePool.getAvailableSlaves();
			logResources(jobs, slaves);
			System.out.println("Amount of jobs in the queue: " + jobs);
			if (counter == 19) {
				counter = 0;
				slidingwindow = slideWindow(slidingwindow, jobs);
				System.out.println("Average: " + averageOfWindow(slidingwindow));
				System.out.println("Availableslaves: " + slaves);
				System.out.println("Pendingslaves: " + pendingVMs.size());
			}
			
			checkPendingVMS();
			
			runPolicy(slaves, slidingwindow, jobs);
			
			
			try {
				Thread.sleep(1000);
				counter++;
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	public void runPolicy(int availableslaves, int[] slidingwindow, int jobsinqueue) {
		if (isStartup) {
			if (resourcePool.size() != minimumslaves)
				return;
			isStartup = false;
		}
		
		if (policy == 1)
			percentRulePolicy(availableslaves, slidingwindow, jobsinqueue);
		else if (policy == 2)
			jobsInQueuePolicy(jobsinqueue, slidingwindow);
		else if (policy == 3)
			availableSlavesPolicy(availableslaves);
		else
			System.exit(50);
	}
	
	public void percentRulePolicy (int availableslaves, int[] slidingwindow, int jobsinqueue) {
		System.out.println("percentRulePolicy");
		int pending = pendingVMs.size();
		int numberofslaves = resourcePool.size();
		if (averageOfWindow(slidingwindow) > (availableslaves + pending)*1.1
										&& numberofslaves+pending < maximumslaves) {
			launchVMs(1);
		} else if (averageOfWindow(slidingwindow) < (availableslaves + pending)*0.9
										&& jobsinqueue < (availableslaves + pending)*0.9
										&& numberofslaves > minimumslaves) {
			shutdownVM();
		} else if (averageOfWindow(slidingwindow) == 0 && numberofslaves > minimumslaves) {
			shutdownVM();
		}
	}
	
	public void jobsInQueuePolicy (int jobsinqueue, int[] slidingwindow) {
		System.out.println("jobsInQueuePolicy");
		int pending = pendingVMs.size();
		int numberofslaves = resourcePool.size();
		if (jobsinqueue > pending && numberofslaves+pending < maximumslaves)  {
			launchVMs(1);
		} else if (averageOfWindow(slidingwindow) == 0 && numberofslaves > minimumslaves) {
			shutdownVM();
		}
	}
	
	public void availableSlavesPolicy (int availableslaves) {
		System.out.println("availableSlavesPolicy");
		int pending = pendingVMs.size();
		int numberofslaves = resourcePool.size();
		if (availableslaves+pending < 0.3*(numberofslaves) && numberofslaves+pending < maximumslaves) {
			launchVMs(1);
		} else if (availableslaves > 0.5*(numberofslaves) && pending == 0 
									&& numberofslaves > minimumslaves) {
			shutdownVM();
		} else if (availableslaves == numberofslaves && pending == 0 && numberofslaves > minimumslaves) {
			shutdownVM();
		}
	}

	public void startLog() {
		String filepath = prop.getProperty("vmlogfile");
		File file = new File(filepath);
		if (file.exists()) {
			int i = 0;
			File replace = new File(file.getPath() + "old" + i + file.getName());
			for (i = 1;replace.exists(); i++) {
				replace = new File(file.getPath() + "old" + i + file.getName());
			}
			file.renameTo(replace);
		}
		
		try
		{
		    FileWriter fw = new FileWriter(filepath, true);
		    fw.write("Timestamp\tPendingVMs\tRunningVMs\tjobsinqueue\tslavesrunning\n");
		    fw.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public void logResources(int queuesize, int availableslaves) {
		String filepath = prop.getProperty("vmlogfile");
		java.util.Date date= new java.util.Date();

		try	{
		    FileWriter fw = new FileWriter(filepath, true);
		    fw.write(new Timestamp(date.getTime()).toString()
		    		+ "\t" + pendingVMs.size()
		    		+ "\t" + resourcePool.size()
		    		+ "\t" + queuesize
		    		+ "\t" + (resourcePool.size() - availableslaves)
		    		+ "\n");
		    fw.close();
		} catch(IOException e) {
			e.printStackTrace();
		}
	}
	
	public static int[] slideWindow(int[] window, int newvalue) {
		int[] temp = new int[5];
		for (int i = 0; i < temp.length-1; i++) {
			temp[i] = window[i+1];
		}
		temp[temp.length-1] = newvalue;

		return temp;
	}
	
	public static int averageOfWindow(int[] window) {
		int total = 0;
		for(int i = 0; i < window.length; i++) {
			total += window[i];
		}
		return total/window.length;
	}

	/**
	 * Handles a failed slave by killing it after a specific amount of time if it hasn't started working by then
	 * @param slaveInstanceID
	 */
	public void handleFailedSlave(String slaveInstanceID) {
		System.out.println("VMHandler removing failed slave "+slaveInstanceID+" from runnning VMs and adding to pending VMs");
		resourcePool.removeSlave(slaveInstanceID);
		pendingVMs.add(slaveInstanceID);
		SlaveKillerThread killer = new SlaveKillerThread(slaveInstanceID, this);
		killer.start();
	}
	
	private void moveSlaveFromPendingToRunning(String host, String id)
	{
		pendingVMs.remove(id);
		resourcePool.addToPool(host, id);
	}

	/**
	 * Kills the given slave if it is still pending (e.g. java has crashed, so we give up on the whole VM)
	 * @param slaveInstanceID
	 */
	public void killIfPending(String slaveInstanceID) {
		boolean res = pendingVMs.remove(slaveInstanceID);
		if(res)
		{
			System.out.println("Removed slave "+slaveInstanceID+" from pendingVMs (It's been there for too long, so java is probably crashed). Shutting VM down");
			shutdownVM(slaveInstanceID);
			launchVMs(1);
		}
		else
		{
			System.out.println("Not removing slave "+slaveInstanceID+" from pending VMs. It appears to have resurrected");
		}
		
	}
}