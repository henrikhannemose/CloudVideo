package client;

import java.util.ArrayList;
import java.util.Random;

import org.uncommons.maths.random.ExponentialGenerator;

public class JobGenerator {

	private long minute=60000;
	ExponentialGenerator g;
	Random fileRandom = new Random(2000);
	static ArrayList<String> inputFiles = new ArrayList<String>();
	boolean randomFiles;
	int fileCounter=0;
	
	public JobGenerator(float jobsPerMinute, boolean randomFiles, boolean smalljobs)
	{
		if(smalljobs) {
			inputFiles.add("anchorman2-tlr2b_h480p.mov");
			inputFiles.add("captainamericathewintersoldier-tsrtlr1_h480p.mov");
			inputFiles.add("The.Avengers.Trailer.1080p.mov");
			inputFiles.add("allwifedout-tlr_h480p.mov");
			inputFiles.add("charliecountryman-tlr_h480p.mov");
			inputFiles.add("nonstop-tlr1_h480p.mov");
			inputFiles.add("philomena-tlr1r_h480p.mov");
			inputFiles.add("thatakwardmoment-tlr1_h480p.mov");
		
			this.randomFiles=randomFiles;
			Random r = new Random(1000); //Same seed each time to have same distribution
			g = new ExponentialGenerator(jobsPerMinute,r);
		}
	}

	/**
	 * Used to generate "random" (the same seed is reused) jobs and timing between jobs
	 * @param jobsPerMinute
	 * @param randomFiles
	 */
	public JobGenerator(float jobsPerMinute, boolean randomFiles)
	{
			inputFiles.add("anchorman2-tlr2b_h480p.mov");
			inputFiles.add("captainamericathewintersoldier-tsrtlr1_h480p.mov");
			inputFiles.add("herecomesthedevil-tlr_h720p.mov");
			inputFiles.add("mrpeabodyandsherman-tlr1_h1080p.mov");
			inputFiles.add("sample_iTunes.mov");
			inputFiles.add("The.Avengers.Trailer.1080p.mov");
			inputFiles.add("themotellife-tlr1_h720p.mov");
			inputFiles.add("allwifedout-tlr_h480p.mov");
			inputFiles.add("charliecountryman-tlr_h480p.mov");
			inputFiles.add("endlesslove-tlr1_h720p.mov");
			inputFiles.add("nonstop-tlr1_h480p.mov");
			inputFiles.add("philomena-tlr1r_h480p.mov");
			inputFiles.add("thatakwardmoment-tlr1_h480p.mov");
			inputFiles.add("paranormalactivitythemarkedones-tlr1_h720p.mov");
			inputFiles.add("TAM_RedBand_DomTrailer1_720p.mov");
			inputFiles.add("onechance-tlr_h1080p.mov");
		
			this.randomFiles=randomFiles;
			Random r = new Random(1000); //Same seed each time to have same distribution
			g = new ExponentialGenerator(jobsPerMinute,r);
	}

	public long timeUntilNextJob()
	{
		return (long)(g.nextValue()*minute);
	}
	public String getFile()
	{
		if(randomFiles)
		{
			return inputFiles.get(fileRandom.nextInt(inputFiles.size()));
		}
		else
		{
			if(fileCounter==inputFiles.size())
			{
				fileCounter=0;
			}
			return inputFiles.get(fileCounter++);
		}
	}
}
