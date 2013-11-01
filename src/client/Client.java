package client;
import global.GlobalVariables;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Properties;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

/**
 * Simple program that connects with the ClientServer and uploads a file
 */
public class Client {
	private String filepath = null;
	private String filename = null;
	private String outputformat = null;
	private long clientID = -1;
	private Socket socket = null;
	private String masteraddress = null;
	private int port = 15050;
	private String bucketname = null;
	private Region region = null;
	private String s3path = null;
	private boolean noUploadMode = false;
	
	/**
	 * Constructor
	 * 
	 * @param fpath		path of the file the client wants to send
	 * @param address	ip/url of the master
	 * @param p			the port of the master
	 */
	public Client(String fpath, String format, String address, int p, boolean noUploadMode) {
		Properties prop = GlobalVariables.getGlobalVariables();
		masteraddress = prop.getProperty("masteraddress");
		port = Integer.parseInt(prop.getProperty("clientserverport"));
		region = Region.fromValue(prop.getProperty("region"));
		filepath = fpath;
		outputformat = format;
		this.noUploadMode=noUploadMode;
		
		System.out.println("Started client. File="+fpath+", "+masteraddress + "    " + port + "    " + region);
	}
	
	public boolean connect() {
		try {
			socket = new Socket(masteraddress, port);
			// Read filename and filesize
	        DataInputStream din = new DataInputStream(socket.getInputStream());
	        clientID = din.readLong();
	        bucketname = din.readUTF();	        
	        return true;
		} catch (Exception e)
		{
			System.out.println("Client unable to connect to master "+masteraddress+" - exiting client!");
			return false;
		}		
	}
	
	public void upload() {
		try {
            File file = new File(filepath);
            filename = file.getName();
            
			s3path="http://s3-eu-west-1.amazonaws.com/" + bucketname +"/";
			if(noUploadMode)
			{
				s3path+= "0/";
				System.out.println("Client running in no-upload mode. Assuming input-file is located in client 0's bucket");
			}
			else
			{
				s3path+=String.valueOf(clientID) + "/";
			
				AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
				s3.setRegion(region.toAWSRegion());
				
				TransferManager tx = new TransferManager(s3);
							
	            PutObjectRequest request = new PutObjectRequest(bucketname,
	            								String.valueOf(clientID) + "/" + filename, file);
	            System.out.println("Uploading output file to S3 (client "+clientID+")");
	            Upload upload = tx.upload(request);
	            upload.waitForCompletion();
	            tx.shutdownNow();
				System.out.println("Client " + clientID + " has dumped the file in the bucket");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public void notifyMaster() {
		try {
			DataOutputStream dos = new DataOutputStream(socket.getOutputStream());
			dos.writeUTF(s3path);
			dos.writeUTF(filename);
			dos.writeUTF(outputformat);
	        dos.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	/**
	 * receiveResponse() waits for a response from the ClientServer
	 */
	public void receiveResponse () {
		try {
	        DataInputStream din = new DataInputStream(socket.getInputStream());
	        String response = din.readUTF();
	        if (!response.equals(":(")) {
	        	System.out.println(":D, it worked! (client "+clientID+", outputFile="+response+")");
	        } else {
	        	System.out.println(":(, it didn't work! (client "+clientID+")");
	        }
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws IOException {
		int p = -1;
		boolean noUploadMode=false;
		if (args.length == 0) {
			// For testing purposes
			args = new String[3];
			args[0] = "Files/sample_iTunes.mov";
			args[1] = "mp4";
			args[2] = "127.0.0.1";
			noUploadMode=true;
			p = 15050;
		} else {
			// For production mode
			if (args.length != 5) {
				System.err.println("Wrong commandline argument(s)");
				System.exit(1);
			}
			
			try {
				p = Integer.valueOf(args[3]);
				noUploadMode=Boolean.valueOf(args[4]);
			} catch (Exception e) {
				System.err.println("Wrong commandline argument(s)");
				System.exit(1);
			}
		}
		
		Client client = new Client(args[0], args[1], args[2], p,noUploadMode);
		
		if(client.connect())
		{
			client.upload();
			client.notifyMaster();
			client.receiveResponse();
		}
		client.socket.close();
	}
}