package master;

import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.HashSet;
import java.util.Set;

import slave.SlaveInterface.ISlave;

public class ResourcePool {
	boolean lock = false;
	Set<SlaveResource> resourcepool = new HashSet<SlaveResource>();
	private VMHandler vmhandler;
	
	private synchronized boolean setLock () {
		if (lock == false) {
			lock = true;
			return lock;
		}
		
		return false;
	}
	
	private void releaseLock() {
		if (lock == true) {
			lock = false;
		}
	}
	
	/**
	 * Add a slave to the pool
	 * @param host Hostname/IP of the slave
	 * @param id Amazon ID of the VM
	 */
	public void addToPool(String host, String id)
	{		
        try{
            Registry registry = LocateRegistry.getRegistry(host);
            ISlave slave = (ISlave) registry.lookup("ISlave");
            slave.setInstanceID(id);
            while(!setLock());
    		resourcepool.add(new SlaveResource(id,slave));
    		System.out.println("Added slave to pool");
        } catch (Exception e) {
            System.err.println("Client exception: " + e.toString());
            e.printStackTrace();
        } finally {
        	releaseLock();
        }
	}
	
	/**
	 * Blocks until a slave is available and returns it
	 * @return
	 */
	public ISlave getAvailableSlave()
	{
		while (!setLock());
		ISlave slave = null;

		for(SlaveResource slaveRes : resourcepool) {
			try{
				if(slaveRes.getISlave().isRunning())
					continue;
				slave = slaveRes.getISlave();
				slave.setRunning();
				break;
			}
			catch(RemoteException e)
			{
				System.out.println("Encountered dead slave in resourcePool. Asking VMHandler to take care of it");
				vmhandler.handleFailedSlave(slaveRes.id);
			}
		}
		releaseLock();
		return slave;
	}
	
	public int getSize(){
		while(!setLock());
		int rval = resourcepool.size();
		releaseLock();
		return rval;
	}

	/**
	 * Returns the amount of available slaves in the pool
	 * @return
	 */
	public int getAvailableSlaves() {
		while(!setLock());
		
		int i = 0;
		try {
			for(SlaveResource slaveRes : resourcepool) {
				if(!slaveRes.getISlave().isRunning())
					i++;
			}
		} catch (RemoteException e) {
			return i; //TODO!
		} finally {
			releaseLock();
		}
		releaseLock();
		return i;
	}

	public void removeSlave(String slaveInstanceID) {
		while(!setLock());
		for (SlaveResource slaveRes : resourcepool) {
			if(slaveRes.getID().equals(slaveInstanceID))
			{
				resourcepool.remove(slaveRes);
				break;
			}
		}
		releaseLock();
	}
	
	public int size()
	{	
		while (!setLock());
		int rval = resourcepool.size();
		releaseLock();
		return rval;
	}

	public void setVMHandler(VMHandler vmhandler) {
		this.vmhandler=vmhandler;
		
	}
}
