package slave;

import java.rmi.Remote;
import java.rmi.RemoteException;

import master.Master;

public interface SlaveInterface {

	public interface ISlave extends Remote {
		/**
		 * Transcodes the given file. Returns when done
		 * @param inputFilePath
		 * @param inputFileName
		 * @param outputFileExtension
		 * @param clientID
		 * @return
		 * @throws RemoteException
		 */
		String runJob(String inputFilePath, String inputFileName, String outputFileExtension, long clientID) throws RemoteException;
		/**
		 * Returns true if a job is currently running
		 * @return
		 * @throws RemoteException
		 */
		boolean isRunning() throws RemoteException;
		void setRunning() throws RemoteException;
		void setInstanceID(String id) throws RemoteException;
		String getInstanceID() throws RemoteException;
		/**
		 * Remote kill switch to use for simulating crashes
		 * @throws RemoteException
		 */
		void killJava() throws RemoteException;
	}
	public void test();
}
