package master;

import com.amazonaws.auth.ClasspathPropertiesFileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;

public class Job {

	private long clientID;
	private String filePath;
	private String fileName;
	private String outputFileExtension;
	private ClientServerThread clientServerThread;
	private String inputFileHash;
	private String actualOutputFile;

	/**
	 * Creates a new transcoding job
	 * @param filePath
	 * @param fileName
	 * @param outputFileExtension
	 * @param clientID
	 * @param clientServerThread
	 * @param enableCaching
	 */
	public Job(String filePath, String fileName, String outputFileExtension, long clientID, ClientServerThread clientServerThread, boolean enableCaching)
	{
		this.filePath=filePath;
		this.fileName=fileName;
		this.outputFileExtension=outputFileExtension;
		this.clientID=clientID;
		this.clientServerThread=clientServerThread;
		
		if(enableCaching)
		{
			AmazonS3 s3 = new AmazonS3Client(new ClasspathPropertiesFileCredentialsProvider());
			s3.setRegion(Region.getRegion(Regions.EU_WEST_1));
			String bucketName = "cloudvideobucket";
					
			String stripString = "http://s3-eu-west-1.amazonaws.com/" + bucketName +"/";
			String s3Key = filePath.substring(stripString.length())+fileName;
			
			GetObjectRequest req = new GetObjectRequest(bucketName, s3Key);
			S3Object obj = s3.getObject(req);
			inputFileHash = obj.getObjectMetadata().getETag();
		}
	}
	
	public long getClientID() {
		return clientID;
	}
	public String getFileName() {
		return fileName;
	}
	public String getOutputFileExtension() {
		return outputFileExtension;
	}
	
	public void notifyClient(String message)
	{
		actualOutputFile=message;
		clientServerThread.notifyClient(message);
	}

	public String getFilePath() {
		return filePath;
	}
	
	public String getInputFileHash()
	{
		return inputFileHash;
	}
	
	/**
	 * Returns true if the other job is identical (same input files, same output format)
	 * @param j
	 * @return
	 */
	public boolean sameOutputFile(Job j)
	{
		return getInputFileHash().equals(j.getInputFileHash()) && getOutputFileExtension().equals(j.getOutputFileExtension());
	}
	
	@Override
	public String toString() {
		return "Job input = "+fileName+", output="+outputFileExtension;
	}
	
	public String getActualOutputFile()
	{
		return actualOutputFile;
	}
}
