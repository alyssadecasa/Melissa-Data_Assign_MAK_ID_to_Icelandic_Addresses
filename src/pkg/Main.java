/**
 * Assign a MAK id to Verified Global Addresses Project
 * Main.java
 * 
 * Inputs: SQL Table 
 *  Table name inputted in WriteToPropertiesFile.java
 * 	MUST HAVE columns spelled as listed: [recID], [Address1], [Address2],[Address3], [Address4],
 * 	[Address5], [Address6], [Address7], [Address8], [Locality], [AdministrativeArea], 
 * 	[PostalCode], [CountryCode], [DependentLocality], [DoubleDependentLocality], 
 * 	[SubAdministrativeArea], [SubNationalArea]
 * 
 * Outputs: updates SQL Table mak_id column 
 * 
 * Program Description: Reads in SQL Table, checks if a given entry 
 * is a valid address up to the building/suite number, gets the parameters
 * returned from Global Address Verification, sends these updated parameters
 * to Global MAK, and then assigns valid entities in the original SQL table
 * a MAK id
 * 
 * @author Alyssa House
 */

package pkg;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class Main {
	public static String databaseTable;
	private static int count = 1;

	public static void main(String[] args) {
		ResultSet resultSet = null;
		String[] currentRequestParameters = new String[21];	
		Properties properties = new Properties();
		try {
			properties.load(new FileInputStream("config.properties"));
			databaseTable = properties.getProperty("table");
		} catch (IOException e) {
			System.out.println("ERROR IOException in main() : "
					+ "Unable to load config.properties.");
			e.printStackTrace();
			System.exit(0);
		}
		
		resultSet = getSqlResultSet();

		do {
			currentRequestParameters = getParametersFromResultSet(resultSet);
			
			if (currentRequestParameters == null) {
				break;
			}

			// Build and send REST Request to GlobalAddress for current request parameters
			String globalAddressRequest = buildGlobalAddressGETRequest(currentRequestParameters);
			
			sendGlobalAddressGETRequest(globalAddressRequest);

			// Check result codes for fully verified building addresses
			String resultCodes = "";
			resultCodes = getResultCodes();
			
			if (addressFullyVerified(resultCodes)) {
				// Build and send REST Request to GlobalMAK for current request parameters and get returned MAK ID
				currentRequestParameters = updateParameters(currentRequestParameters);
				currentRequestParameters[20] = sendGlobalMakPOSTRequest(currentRequestParameters);
				
				// Update SQL Table with MAK ID
				updateSqlWithMAK(currentRequestParameters[0], currentRequestParameters[20]);
			}
			
			System.out.println(count++);
			
			
		} while (currentRequestParameters != null);

		// Clean: remove JSONResponse.json
		try {
			Files.deleteIfExists(Paths.get("JSONResponse.json"));
		} catch (NoSuchFileException e) {
			System.out.println("ERROR NoSuchFileException : Unable to locate JSONResponse.json");
		} catch (IOException e) {
			System.out.println("ERROR IOException : Unable to delete JSONResponse.json");
			e.printStackTrace();
		}

		
		System.out.println("Program successfully executed.");
	}
	
	/**
	 * Sends a query to SQL Table and returns a ResultSet containing every distinct entry
	 * @return ResultSet containing every distinct entry
	 */
	private static ResultSet getSqlResultSet() {

		//String connectionString = "jdbc:sqlserver:<server name>;" + "database=<database>;" + "user=<user>;" + "password=<password>;";
		ResultSet resultSet = null;
		Statement statement = null;
		Connection connection = null;

		try {
			connection = DriverManager.getConnection(connectionString);
			String selectSql = "SELECT DISTINCT [recID], [Address1],[Address2],[Address3]"
					+ ",[Address4],[Address5],[Address6],[Address7],[Address8],[Locality]"
					+ ",[AdministrativeArea],[PostalCode],[CountryCode],[DependentLocality]"
					+ ",[DoubleDependentLocality],[SubAdministrativeArea],[SubNationalArea]"
					+ "FROM " + databaseTable;
			statement = connection.createStatement();
			resultSet = statement.executeQuery(selectSql);
		} catch (SQLException e) {
			System.out.println("ERROR SQLException in getSqlResultSet() : Unable to query for REST Request parameters.");
			e.printStackTrace();
			System.exit(0);
		}

		return resultSet;
	}

	/**
	 * Parses current result set row for the parameters needed to send a GET Request to the Global Address
	 * Web Service
	 * @param resultSet
	 * @return String array of parameters
	 */
	private static String[] getParametersFromResultSet(ResultSet resultSet) {
		String[] parameters = new String[21];
		
		try {
			while (resultSet.next()) {
				for (int i = 0; i < 16; i++) {
					parameters[i] = resultSet.getString(i + 1).toString().trim();
				}
				
				for (int i = 16; i < 21; i++) {
					parameters[i] = "";
				}
				return parameters;
			}
		} catch (SQLException e) {
			System.out.println("ERROR : SQLException in getParametersFromResultSet() : "
					+ "Unable to get parameters from result set.");
			e.printStackTrace();
			System.exit(0);
		}

		return null;
	}

	/**
	 * Builds GET Request for Global Address Web Service with given parameters
	 * @param parameters
	 * @return String formatted GET Request
	 */
	private static String buildGlobalAddressGETRequest(String[] parameters) {
		//Initialize fields for REST Request
		String request = "";
		String custID = ""; // Input customer ID here
		String address1 = parameters[1];
		String address2 = parameters[2];
		String address3 = parameters[3];
		String address4 = parameters[4];
		String address5 = parameters[5];
		String address6 = parameters[6];
		String address7 = parameters[7];
		String address8 = parameters[8];
		String locality = parameters[9];
		String administrativeArea = parameters[10];
		String postalCode = parameters[11];
		String country = parameters[12];
		String dependentLocality = parameters[13];
		String doubleDependentLocality = parameters[14];
		String subAdministrativeArea = parameters[15];
		String subNationalArea = parameters[16];
		String organization = parameters[17];
		
		// Build request string
		request = "?id=" + custID + "&a1=" + address1 + "&a2=" + address2 + "&a3=" + address3 + "&a4=" + address4
				+ "&a5=" + address5 + "&a6=" + address6 + "&a7=" + address7 + "&a8=" + address8 + "&ddeploc=" 
				+ doubleDependentLocality + "&deploc=" + dependentLocality + "&loc=" + locality + "&subadmarea=" 
				+ subAdministrativeArea + "&admarea=" + administrativeArea + "&postal=" + postalCode + "&subNationalArea=" 
				+ subNationalArea + "&ctry=" + country + "&org=" + organization + "&format=json";
		
		return request;	
	}
	
	/**
	 * Sends GET Request to Global Address Web Service
	 * @param request
	 */
	private static void sendGlobalAddressGETRequest(String request) {
		String httpAddress = "//address.melissadata.net/V3/WEB/GlobalAddress/doGlobalAddress";

		// Create URI
		URI uri = null;
		try {
			uri = new URI("http", httpAddress + request, null);
		} catch (URISyntaxException e) {
			System.out.println("ERROR : URISyntaxException in sendGlobalAddressGETRequest() : " 
					+ "Unable to build URI.");
			e.printStackTrace();
			System.exit(0);
		}

		// Create URL
		URL url = null;
		try {
			url = new URL(uri.toURL().toString());
		} catch (MalformedURLException e) {
			System.out.println("ERROR MalformedURLException in sendGlobalAddressGETRequest() : " 
					+ "Unable to build URL.");
			e.printStackTrace();
			System.exit(0);
		}

		// Loop until either GET request is sent and received or tries exceed 5
		boolean requestRetrieved = false;
		int retry = 0;

		do {
			// Open a Connection
			HttpURLConnection urlConn = null;
			try {
				urlConn = (HttpURLConnection) (url.openConnection());
			} catch (IOException e) {
				System.out.println("ERROR IOException in sendGlobalAddressGETRequest() :" 
						+ " Unable to open connection to URL.");
			}
			
			// Read in the JSON response and write it into a file
			InputStreamReader inputStreamReader = null;
			BufferedReader jsonResponse = null;
			FileWriter jsonFile = null;
			String readLine = "";
			String jsonString = "";

			// Connect and read in GET Response
			try {
				urlConn.connect();
				inputStreamReader = new InputStreamReader(urlConn.getInputStream());
				jsonResponse = new BufferedReader(inputStreamReader);
				jsonFile = new FileWriter("JSONResponse.json");
				while ((readLine = jsonResponse.readLine()) != null) {
					jsonString += readLine;
				}

				jsonFile.write(jsonString);
				jsonFile.flush();
				requestRetrieved = true;
			} catch (IOException e) {
				retry++;
			} finally {

				try {
					if (jsonFile != null) {
						jsonFile.close();
					}
					if (jsonResponse != null) {
						jsonResponse.close();
					}
					if (inputStreamReader != null) {
						inputStreamReader.close();
					}
					urlConn.disconnect();
				} catch (IOException | NullPointerException e) {
					e.printStackTrace();
				}
			}
		} while (requestRetrieved == false && retry < 5);

		if (retry >= 5) {
			System.out.println("ERROR in sendGlobalAddressGETRequest : " 
					+ "Unable to send request after five tries.");
		}
	}
	
	/**
	 * Gets the result codes from the most recent Global Address GET response
	 * @return comma split string containing list of result codes returned
	 */
	private static String getResultCodes() {
		String resultCodes = "";
		FileReader fileReader = null;
		JSONObject jsonResponseObj = null;
		JSONObject recordsObj = null;
		JSONArray records;

		// Create jsonResponseObj from JSON file
		try {
			fileReader = new FileReader("JSONResponse.json"); 
			jsonResponseObj = (JSONObject) new JSONParser().parse(fileReader);
		} catch (IOException | ParseException e) {
			System.out.println("ERROR : IOException or ParseException in getResultCodes() : "
					+ "Unable to create JSONObject from parsing JSONResponse.json.");
			e.printStackTrace();
			System.exit(0);
		}

		// Create subset of jsonResponseObj containing the pairs in the Records array
		records = ((JSONArray) jsonResponseObj.get("Records"));
		recordsObj = (JSONObject) records.get(0);

		// Get result codes
		resultCodes = recordsObj.get("Results").toString();

		try {
			fileReader.close();
		} catch (IOException e) {
			System.out.println("ERROR IOException in getResultCodes() :"
					+ "Unable to close fileReader.");
			e.printStackTrace();
			System.exit(0);
		}
		return resultCodes;
	}
	
	/**
	 * Determines if address returned from most recent Global Address GET response
	 * is fully verified to the building and/or suite level
	 * @param resultCodes
	 * @return true if fully verified to the building and/or suite level, false otherwise
	 */
	private static boolean addressFullyVerified(String resultCodes) {
		String[] codes = resultCodes.split(",");
		
		for (String code : codes) {
			if (code.equals("AV24") || code.equals("AV25")) {
				return true;
			}
		}
		// Address was not fully verified at either building level or suite level
		return false;
	}

	/**
	 * Sets each of the values of the given parameters string array to the values returned by the 
	 * Global Address Web Request in proper order
	 * @param parameters 
	 * @return string array containing updated parameters
	 */
	private static String[] updateParameters(String[] parameters) {
		FileReader fileReader = null;
		JSONObject jsonResponseObj = null;
		JSONObject recordsObj = null;
		JSONArray records;

		// Create jsonResponseObj from JSON file
		try {
			fileReader = new FileReader("JSONResponse.json"); 
			jsonResponseObj = (JSONObject) new JSONParser().parse(fileReader);
		} catch (IOException | ParseException e) {
			System.out.println("ERROR : IOException or ParseException in updateParameters() : "
					+ "Unable to create JSONObject from parsing JSONResponse.json.");
			e.printStackTrace();
			System.exit(0);
		}

		// Create subset of jsonResponseObj containing the pairs in the Records array
		records = ((JSONArray) jsonResponseObj.get("Records"));
		recordsObj = (JSONObject) records.get(0);
		
		// Get updated parameters from Global Address Verification Web Service
		parameters[1] = recordsObj.get("AddressLine1").toString();
		parameters[2] = recordsObj.get("AddressLine2").toString();
		parameters[3] = recordsObj.get("AddressLine3").toString();
		parameters[4] = recordsObj.get("AddressLine4").toString();
		parameters[5] = recordsObj.get("AddressLine5").toString();
		parameters[6] = recordsObj.get("AddressLine6").toString();
		parameters[7] = recordsObj.get("AddressLine7").toString();
		parameters[8] = recordsObj.get("AddressLine8").toString();
		parameters[9] = recordsObj.get("Locality").toString();
		parameters[10] = recordsObj.get("AdministrativeArea").toString();
		parameters[11] = recordsObj.get("PostalCode").toString();
		parameters[12] = recordsObj.get("CountryName").toString();
		parameters[13] = recordsObj.get("DependentLocality").toString();
		parameters[14] = recordsObj.get("DoubleDependentLocality").toString();
		parameters[15] = recordsObj.get("SubAdministrativeArea").toString();
		parameters[16] = recordsObj.get("SubNationalArea").toString();
		parameters[17] = recordsObj.get("Thoroughfare").toString();
		parameters[18] = recordsObj.get("PremisesNumber").toString();
		parameters[19] = recordsObj.get("SubPremisesNumber").toString();
		
		try {
			fileReader.close();
		} catch (IOException e) {
			System.out.println("ERROR IOException in getResultCodes() : "
					+ " Unable to close fileReader");
			e.printStackTrace();
			System.exit(0);
		}
		
		return parameters;
	}
	
	/**
	 * builds a POST Request to the Global MAK Web Service with the given parameters
	 * @param parameters
	 * @return JSON formatted string containing the POST Request fields
	 */
	@SuppressWarnings("unchecked")
	private static String buildGlobalMakPOSTRequest(String[] parameters) {
		JSONObject request = new JSONObject();
		String jsonString = "";
		
		// Initialize fields for REST Request
		String address1 = parameters[1];
		String address2 = parameters[2];
		String address3 = parameters[3];
		String address4 = parameters[4];
		String address5 = parameters[5];
		String address6 = parameters[6];
		String address7 = parameters[7];
		String address8 = parameters[8];
		String locality = parameters[9];
		String administrativeArea = parameters[10];
		String postalCode = parameters[11];
		String country = parameters[12];
		String dependentLocality = parameters[13];
		String doubleDependentLocality = parameters[14];
		String subAdministrativeArea = parameters[15];
		String subNationalArea = parameters[16];
		String thoroughfare = parameters[17];
		String premisesNumber = parameters[18];
		String subPremisesNumber = parameters[19];

		// Fill in requestString
		request.put("AddressLine1", address1);
		request.put("AddressLine2", address2);
		request.put("AddressLine3", address3);
		request.put("AddressLine4", address4);
		request.put("AddressLine5", address5);
		request.put("AddressLine6", address6);
		request.put("AddressLine7", address7);
		request.put("AddressLine8", address8);
		request.put("Locality", locality);
		request.put("AdministrativeArea", administrativeArea);
		request.put("PostalCode", postalCode);
		request.put("Country", country);
		request.put("DependentLocality", dependentLocality);
		request.put("DoubleDependentLocality", doubleDependentLocality);
		request.put("SubAdministrativeArea", subAdministrativeArea);
		request.put("SubNationalArea", subNationalArea);
		request.put("Thoroughfare", thoroughfare);
		request.put("PremisesNumber", premisesNumber);
		request.put("SubPremisesNumber", subPremisesNumber);
		request.put("iso2", "IS");

		jsonString = request.toString();
		return jsonString;
	}
	
	/**
	 * Sends a POST Request to the Global MAK Web Service with the given parameters and returns the
	 * MAK id received from the POST Response
	 * @param parameters
	 * @return string formatted MAK id received from POST Response
	 */
	private static String sendGlobalMakPOSTRequest(String[] parameters) {
		String mak = "";
		CloseableHttpClient httpClient = HttpClientBuilder.create().build();
		HttpPost post = new HttpPost(""); // global MAK web API
		
		// Build POST request
		String request = buildGlobalMakPOSTRequest(parameters);
		StringEntity entity = new StringEntity(request, ContentType.APPLICATION_JSON);
		post.addHeader("content-type", "application/json");
		post.setEntity(entity);
		
		// Execute POST Request
		HttpResponse response = null;
		try {
			response = httpClient.execute(post);
			
			if (response.getStatusLine().getStatusCode() != 200) {
				System.out.println("ERROR in sendGlobalMakPOSTRequest() : "
						+ "Returned response code is not 200 (Success) : " + response.getStatusLine().getStatusCode());
			}
			
			if (response.getStatusLine().getStatusCode() == 200) {
				// Get MAK ID
				mak = getMAK(response);
			}
		} catch (IOException e) {
			System.out.println("ERROR IOException in sendGlobalMakRESTRequest() :"
					+ " Unable to execute post method.");
			 e.printStackTrace();
			 System.exit(0);
		} finally {
			post.releaseConnection();
		}
		
		return mak;
	}
	
	/**
	 * Gets the MAK id of the given Global MAK REST response
	 * @param response
	 * @return string formatted MAK id
	 */
	private static String getMAK(HttpResponse response) {
		HttpEntity entity = response.getEntity();
		String responseString = "";
		String mak = "";
		
		//Convert Global MAK response into string format
		try {
			responseString = EntityUtils.toString(entity, "UTF-8");
		} catch (org.apache.http.ParseException e) {
			System.out.println("ERROR org.apache.http.ParseException in getMAK() : "
					+ "Unable to convert Global MAK response into string format.");
			e.printStackTrace();
			System.exit(0);
		} catch (IOException e) {
			System.out.println("ERROR IOException in getMAK() : "
					+ "Unable to convert Global MAK response into string format.");
			e.printStackTrace();
			System.exit(0);
		}
				
		// Convert Global MAK response string into JSON Object format
		JSONObject jsonResponseObject = null;
		try {
			jsonResponseObject = (JSONObject) new JSONParser().parse(responseString);
		} catch (ParseException e) {
			System.out.println("ERROR ParseException in getMAK() : "
					+ "Unable to parse response from Global MAK.");
			e.printStackTrace();
		}
		
		// Get MAK ID
		mak = jsonResponseObject.get("_id").toString();
		
		return mak;
	}

	/**
	 * Updates the SQL Table's mak_id column by finding the row with the given record ID and
	 * updating that row's mak_id to the given MAK id
	 * @param recordID
	 * @param mak
	 */
	private static void updateSqlWithMAK(String recordID, String mak) {
		//String connectionString = "jdbc:sqlserver:<server name>;" + "database=<database>;" + "user=<user>;" + "password=<password>;";
		Statement statement = null;
		Connection connection = null;

		try {
			connection = DriverManager.getConnection(connectionString);
			String sqlQuery =  "update " + databaseTable
					  + " set mak_id = \'" + mak + "\' where recID = \'" + recordID + "\'";
			statement = connection.createStatement();
			statement.execute(sqlQuery);
		} catch (SQLException e) {
			System.out.println("ERROR SQLException in updateSqlWithMAK : Unable to execute query to update singular MAK ID.");
			e.printStackTrace();
			System.exit(0);
		}
	}
}
