package global;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class GlobalVariables {
	
	/**
	 * getGlobalVariables() reads all the variables in the globalvariable.properties
	 * file, and returns them in the form of a Properties object.
	 * 
	 * @return			A properties object containing all global variables, or null
	 * 					if reading fails
	 */
	public static Properties getGlobalVariables() {
		Properties prop = new Properties();
		try {
			prop.load(new FileInputStream("src/globalvariables.properties"));
		} catch (IOException e) {
			return null;
		}		
		return prop;
	}
}