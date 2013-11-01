package master;

import global.GlobalVariables;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

/**
 * 
 *	ClientServerThread handles the requests of a single
 *	Client.
 *
 */
public class ClientServerThread extends Thread {
    private Socket socket = null;
    private Master master = null;
    private Job job;
    private long clientID = -1;

    public ClientServerThread(Socket s, Master m) {
        super("ClientServerThread");
        socket = s;
        master = m;
    }
    
    public void uploadInfo() {
    	try {
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			clientID = master.getClientID();
			dos.writeLong(clientID);
	        dos.writeUTF(GlobalVariables.getGlobalVariables().getProperty("bucketname"));
	        dos.flush();	        
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void getJobInfo() {
    	try {
			DataInputStream din = new DataInputStream(socket.getInputStream());
			String filePath = din.readUTF();
	        String filename = din.readUTF();
	        String outputformat = din.readUTF();

	        job = new Job(filePath,filename,outputformat,clientID,this, master.getEnableJobCaching());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }
    
    public void runSlave() {
    	master.enqueueAndRunJob(job);
    }
    
    public void notifyClient(String message) {
		try {
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.writeUTF(message);
	        dos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    }

    public void run() {
		uploadInfo();
		getJobInfo();
		runSlave();
    }
}