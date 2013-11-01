package client;

import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ClientSimulator {

	static String masterIP="127.0.0.1";
	static int port=15050;

	public static void main(String[] args) throws InterruptedException, IOException {
		
		//backToBackExperimentRunner("TinyImage.png","tiff",100,1); //Use to measure overhead of system 
		//backToBackExperimentRunner("sample_iTunes.mov","mp4",8,1); //Use for comparing performance with and without uploading files instantly
		exponentialJobTimeExperimentRunner(3, 15); //Use for realistic workloads, see how VMHandler performs
		return;
	}
	
	/**
	 * Executes a series of jobs using an exponential distribution
	 * @param jobsPerMinute
	 * @param totalJobs
	 * @throws InterruptedException
	 * @throws IOException
	 */
	private static void exponentialJobTimeExperimentRunner(int jobsPerMinute, int totalJobs) throws InterruptedException, IOException
	{
		FileWriter csvWriter = new FileWriter("timingfile.csv");
		FileWriter csvWriter2 = new FileWriter("timingfilePerJob.csv");
		List<ClientSimulatorThread> threads = new ArrayList<ClientSimulatorThread>();
		System.out.println("Starting simulation");
		long startTime = System.currentTimeMillis();
		
		JobGenerator g = new JobGenerator(jobsPerMinute, false, true);
		for(int i=0;i<totalJobs;i++)
		{
			ClientSimulatorThread t = new ClientSimulatorThread(g.getFile(),"mp4",masterIP,port,true);
			threads.add(t);
			t.start();
			if(i==totalJobs-1)
			{
				System.out.println("Not sleeping. Last client");
				break;
			}
			long sleepTime = g.timeUntilNextJob();
			System.out.println("Sleeping "+sleepTime+" ms until next job");
			Thread.sleep(sleepTime);
		}
		for (ClientSimulatorThread t : threads) {
			t.join();
		}
		long endTime = System.currentTimeMillis();
		long duration = endTime-startTime;
		System.out.println("Simulation ended. Total duration = "+duration+" ms.");
		for (ClientSimulatorThread t : threads) {
			System.out.println("Duration of thread "+t.getId()+"="+t.getDuration());
			csvWriter2.write(t.getDuration()+System.lineSeparator());
		}
		csvWriter.write(duration+System.lineSeparator());
		csvWriter2.write(System.lineSeparator());
		csvWriter.close();
		csvWriter2.close();
	}

	/**
	 * Runs numberPerBatch jobs of the given file and records the timing. Repeated numberOfBatches times
	 * @param fileName
	 * @param outFileExtension
	 * @param numberPerBatch
	 * @param numberOfBatches
	 * @throws IOException
	 * @throws InterruptedException
	 */
	@SuppressWarnings("unused")
	private static void backToBackExperimentRunner(String fileName, String outFileExtension, int numberPerBatch, int numberOfBatches)
			throws IOException, InterruptedException {
		FileWriter csvWriter = new FileWriter("timingfile.csv");
		FileWriter csvWriter2 = new FileWriter("timingfilePerJob.csv");
		
		for(int i=0;i<numberOfBatches;i++)
		{
			backToBackExperiment(fileName, outFileExtension, masterIP, port, numberPerBatch, csvWriter,csvWriter2);
		}
		csvWriter.close();
		csvWriter2.close();
	}

	private static void backToBackExperiment(String fileName, String outFileExtension, String masterIP, int port, int numClients, FileWriter csvWriter,FileWriter csvWriter2)
			throws InterruptedException, IOException {
		List<ClientSimulatorThread> threads = new ArrayList<ClientSimulatorThread>();
		System.out.println("Starting simulation");
		long startTime = System.currentTimeMillis();
		for(int i=0;i<numClients;i++)
		{
			ClientSimulatorThread t = new ClientSimulatorThread(fileName,outFileExtension,masterIP,port,true);
			threads.add(t);
			t.start();
		}
		for (ClientSimulatorThread t : threads) {
			t.join();
		}
		long endTime = System.currentTimeMillis();
		long duration = endTime-startTime;
		System.out.println("Simulation ended. Total duration = "+duration+" ms.");
		for (ClientSimulatorThread t : threads) {
			System.out.println("Duration of thread "+t.getId()+"="+t.getDuration());
			csvWriter2.write(t.getDuration()+System.lineSeparator());
		}
		csvWriter.write(duration+System.lineSeparator());
		csvWriter2.write(System.lineSeparator());
	}
}