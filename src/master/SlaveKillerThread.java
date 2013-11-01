package master;

public class SlaveKillerThread extends Thread{
	
	int timeBeforeKill=60;//seconds before we give up on a slave
	
	String slaveInstanceID;
	VMHandler handler;
	
	/**
	 * Kills a slave if it is not responding after timeBeforeKill seconds
	 * @param slaveInstanceID
	 * @param handler
	 */
	public SlaveKillerThread(String slaveInstanceID, VMHandler handler)
	{
		this.slaveInstanceID=slaveInstanceID;
		this.handler=handler;
	}
    public void run() 
    {
    	try {
			Thread.sleep(timeBeforeKill*1000);
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	handler.killIfPending(slaveInstanceID);
    }
}
