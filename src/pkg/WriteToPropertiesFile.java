/**
 * Assign a MAK id to Verified Global Addresses Project
 * WriteToPropertiesFile.java
 * 
 * Program description: writes config.properties file which contains the properties for
 * this project. Currently the only property is table, which defines the SQL Table to be
 * used as input and updated with a mak ID
 */


package pkg;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Properties;

public class WriteToPropertiesFile {

	public static void main (String[] args) {
		Properties prop = new Properties();
		OutputStream output = null;

		try {

			output = new FileOutputStream("config.properties");

			// set the properties value
			prop.setProperty("table", "");	// set table

			// save properties to project root folder
			prop.store(output, null);

		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}

		}
		System.out.println("config.properties written.");
	}

}
