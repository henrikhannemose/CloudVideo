package slave;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.rmi.RemoteException;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;

import slave.SlaveInterface.ISlave;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class Slave implements ISlave{

	private String outputRoot="/home/ec2-user/CloudVideo/Server/";
	private boolean isRunning;
	private boolean runNewJobsWhileUploadingToS3 = false;
	private String instanceID = "";
	
	public Slave()
	{

	}
	
    public static void main(String[] args) {
        try{
        	Slave obj = new Slave();
            ISlave stub = (ISlave) UnicastRemoteObject.exportObject(obj, 0);

            Registry registry = LocateRegistry.createRegistry(1099);
            registry.bind("ISlave", stub);
            System.err.println("Slave ready. runNewJobsWhileUploadingToS3="+obj.runNewJobsWhileUploadingToS3);
            while(true)
            {
            	BufferedReader bufferRead = new BufferedReader(new InputStreamReader(System.in));
        	    String s = bufferRead.readLine();
        	    if(s==null) break;
        	    if(s.equals("info"))
        	    {
        	    	System.out.println("instanceID="+obj.instanceID);
        	    	System.out.println("isRunning="+obj.isRunning);
        	    	System.out.println("runNewJobsWhileUploadingToS3="+obj.runNewJobsWhileUploadingToS3);
        	    }
        	    else if(s.equals("kill"))
        	    {
        	    	obj.killJava();
        	    }
        	    else
        	    {
        	    	System.out.println("Unknown command");
        	    }
            }
        } catch (Exception e) {
            System.err.println("Slave exception: " + e.toString());
            e.printStackTrace();
        }
    }

    @Override
    public String runJob(String inputFilePath, String inputFileName, String outputFileExtension, long clientID) {
    	isRunning=true;
    	
    	File outputFile = getOutputFile(inputFileName, outputFileExtension);
    	
		ProcessBuilder pb;
		try {
			AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
			s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
			String bucketName = "cloudvideobucket";

			pb = new ProcessBuilder(
					"ffmpeg","-n","-i", inputFilePath + inputFileName, outputFile.toString());
			pb.redirectErrorStream(true);
			Process p = pb.start();
			InputStream is = p.getInputStream();
			InputStreamReader isr = new InputStreamReader(is);
			BufferedReader br = new BufferedReader(isr);
			String line;

			System.out.println("Started job on slave");

			while ((line = br.readLine()) != null) {
			  System.out.println(line);
			  if(line.contains("No such file or directory") || line.contains("already exists. Exiting"))
			  {
				  System.out.println("Error. Aborting processing");
				  isRunning=false;
				  p.destroy();
				  return null;
			  }
			}
			p.waitFor();
			System.out.println("Finished job on slave with exit value: "+p.exitValue());
			
        	//AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
        	//AmazonS3 s3 = new AmazonS3Client(new BasicAWSCredentials("AKIAIFJYEHXLJWEVKBSA", "l+YlJVmIP5OEkbb9YU4yPk54y8g4tLjrPUaBiwRB"));
        	//AmazonS3 s3 = new AmazonS3Client(new EnvironmentVariableCredentialsProvider());
			
    		//s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
            //String bucketName = "henrikbucket";
        	if(runNewJobsWhileUploadingToS3)
        	{
        		System.out.println("runNewJobsWhileUploadingToS3=true: Ready to run a new job while uploading");
        		isRunning=false;
        	}
            TransferManager tx = new TransferManager(s3);
            PutObjectRequest request = new PutObjectRequest(bucketName,
            										String.valueOf(clientID) + "/" + outputFile.getName(), outputFile);
            System.out.println("Uploading output file to S3");
            Upload upload = tx.upload(request);
            upload.waitForCompletion();
            System.out.println("Upload done. Removing file from local file system");
            outputFile.delete();
            System.out.println("Done processing job on slave. Returning");
            System.out.println(upload.isDone());
        	if(!runNewJobsWhileUploadingToS3)
        	{
                isRunning=false;
        	}
            if(p.exitValue()==0)
            {
            	return outputFile.getName();
            }
            else
            {
            	return null;
            }
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isRunning=false;
			return null;
			} 
		catch (AmazonServiceException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isRunning=false;
			return null;
		} catch (AmazonClientException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isRunning=false;
			return null;
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			isRunning=false;
			return null;
		}
	}

	private File getOutputFile(String inputFile, String outputFileExtension) {
		int offset=0;
    	while(true)
    	{
    		String outputFileStr = inputFile.substring(0, inputFile.lastIndexOf('.'));
    		if(offset>0)
    		{
        		outputFileStr+="_"+offset;
    		}
    		offset++;
    		outputFileStr+="."+outputFileExtension;
    		File outputFile = new File(outputRoot+outputFileStr);
    		if(!outputFile.exists())
    		{
    				return outputFile;
    		}
    	}
	}

	@Override
	public boolean isRunning() throws RemoteException {
		return isRunning;
	}

	@Override
	public void setRunning() {
		isRunning=true;
	}
	
	@Override
	public void setInstanceID(String id) {
		instanceID = id;
	}
	
	@Override
	public String getInstanceID() {
		return instanceID;
	}

	@Override
	/**
	 * Used to simulate the slave process dying
	 */
	public void killJava() {
		System.out.println("killJava called. Killing JVM right now!");
		System.exit(1);
		
	}
}
