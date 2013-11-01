package master;

import slave.SlaveInterface.ISlave;

public class SlaveResource {

	String id;
	ISlave iSlave;
	
	/**
	 * Identifies a slave in the ResourcePool by its Amazon ID and the RMI object
	 * @param id
	 * @param iSlave
	 */
	public SlaveResource(String id, ISlave iSlave)
	{
		this.id=id;
		this.iSlave=iSlave;
	}
	
	public ISlave getISlave()
	{
		return iSlave;
	}

	public String getID() {
		return id;
	}
}
