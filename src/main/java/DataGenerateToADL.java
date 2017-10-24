import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.security.SecureRandom;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Random;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

public class DataGenerateToADL {
	static XSSFRow row;
	private Config config;
	private AccessTokenProvider provider;
    private ADLStoreClient client;
    public DataGenerateToADL(Config config){
    	this.config=config;
    	this.provider = new ClientCredsTokenProvider(config.getAuthTokenEndpoint(), config.getClientId(),config.getClientKey());;
    	this.client = ADLStoreClient.createClient(config.getAccountFQDN(), provider);
    }
    
    private void generateData(Long noOfRecords, String pathADL, int noOfThreads){
    	FileInputStream fis = null;
		ArrayList<String> fieldName = null;
		ArrayList<String> fieldDataType = null;
		int rowElements = 0;
		PrintStream pout=null;
		
		Random randomno = new Random();
		Long numberOfRecords = noOfRecords / noOfThreads;
		
		
		try {
			// Get Header for CSV file
			fis = new FileInputStream(new File("src/main/resources/DataTemplate.xlsx"));
			XSSFWorkbook workbook = new XSSFWorkbook(fis);
			XSSFSheet spreadsheet = workbook.getSheetAt(0);
			Iterator<Row> rowIterator = spreadsheet.iterator();
			fieldName = new ArrayList<>();
			fieldDataType = new ArrayList<>();
			while (rowIterator.hasNext()) {
				row = (XSSFRow) rowIterator.next();
				Iterator<Cell> cellIterator = row.cellIterator();
				while (cellIterator.hasNext()) {
					Cell cell = cellIterator.next();
					if (row.getRowNum() > 0) {
						if (cell.getColumnIndex() == 0) {
							fieldName.add(cell.getStringCellValue());
						}
						if (cell.getColumnIndex() == 1) {
							fieldDataType.add(cell.getStringCellValue());
						}
					}
				}
			}
			
			// Create Output Stream to ADL
			//client.createDirectory(pathADL.substring(0, pathADL.lastIndexOf('/')-1));
	        String filename = pathADL;
	        OutputStream stream = client.createFile(filename, IfExists.OVERWRITE  );
	        pout = new PrintStream(stream);
	        
	        // Insert Header in CSV
            for (int i = 0; i < fieldName.size(); i++) {
				if (i != (fieldName.size() - 1)) {
					pout.write((fieldName.get(i) + ",").getBytes());
					//pout.flush();
				} else {
					if (i == (fieldName.size() - 1)) {
						pout.write(fieldName.get(i).getBytes());
						//pout.flush();
					} else {
						pout.write((fieldName.get(i) + ",").getBytes());
					//	pout.flush();
					}
				}
				rowElements++;
			}
			pout.write("\n".getBytes());
			
			// Insert numberOfRecords Rows in File
			ArrayList<Thread> threads= new ArrayList<Thread>();
			for (int i = 0; i < noOfThreads; i++) {
				ArrayList<Object> rowPopulation = new ArrayList<>();
				Thread t = new ThreadDataInsertionInADLFile(pout, fieldName,
						fieldDataType, rowElements, new ArrayList<>(), randomno,
						numberOfRecords);
				t.start();
				threads.add(t);
			}
			for(Thread t: threads){
				t.join();
			}
			client.setPermission(pathADL, "777");
			pout.close();
			fis.close();
		} catch (FileNotFoundException | InterruptedException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
			
		} finally {
			System.out.println("Executed Finally");
			System.out.println(System.currentTimeMillis());
		}
    }
	public static void main(String[] args) throws Exception {	
		/*
		 * Arguments to change
		 */
		if(args.length<7)
    		throw new Exception("Please pass required Parameters: AccountFQDN\t"
    				+ "TenantId\t"+"ClientId\t"+"ClientSecret\t"+ "NoOfRecords\t" + "OutputFilePathOnADL\t" + "NoOfThreads");
		Long noOfRecords = (long) 250000000;
		String pathADL = "/regress-download/dataFile32GB.csv";
		int noOfThreads=5;
		noOfRecords=Long.parseLong(args[4]);
		pathADL=args[5];
		noOfThreads=Integer.parseInt(args[6]);
		Config config= new Config().setAccountName(args[0]).setTenantId(args[1]).setClientId(args[2]).setClientKey(args[3]).build();	
		DataGenerateToADL generate= new DataGenerateToADL(config);
		generate.generateData(noOfRecords, pathADL, noOfThreads);
		
	}
}

class ThreadDataInsertionInADLFile extends Thread {

	ArrayList<String> fieldName;
	ArrayList<String> fieldDataType;
	int rowElements;
	ArrayList<Object> rowPopulation;
	Random randomno = new Random();
	PrintStream pout;
	Long noOfRecords;
	String startTime;
	String endTime;
	static HashSet<Integer> hs = new HashSet<Integer>();
	static HashSet<Integer> hs1 = new HashSet<Integer>();

	public ThreadDataInsertionInADLFile(PrintStream pout, ArrayList<String> fieldName,
			ArrayList<String> fieldDataType, int rowElements,
			ArrayList<Object> rowPopulation, Random randomno,
			Long numberOfRecords) {

		this.fieldName = fieldName;
		this.fieldDataType = fieldDataType;
		this.rowElements = rowElements;
		this.rowPopulation = rowPopulation;
		this.randomno = randomno;
		this.pout = pout;
		this.noOfRecords = numberOfRecords;

	}

	public void run() {

		try {

			long rowsToBeinserted = 0;
			for (int i = 0; i < fieldDataType.size(); i++) {
				if (fieldDataType.get(i).contains("int")) {
					rowPopulation.add(Math.abs(randomno.nextInt()));
				} else if (fieldDataType.get(i).contains("char")
						|| fieldDataType.get(i).contains("String")) {
					this.rowPopulation.add(generateRandomString());
				} else if (fieldDataType.get(i).contains("date")
						|| fieldDataType.get(i).contains("time")) {
					rowPopulation.add(generateRandomDate());
				} else if (fieldDataType.get(i).contains("double")) {
					this.rowPopulation.add(generateRandomfloat());

				}
			}

			StringBuilder sb = new StringBuilder();

			for (; rowsToBeinserted < noOfRecords; rowsToBeinserted++) {
				for (int i = 0; i < this.rowElements - 1; i++) {
					if (rowPopulation.get(i) instanceof Integer) {
						rowPopulation.add(Math.abs(randomno.nextInt()));
					} else if (rowPopulation.get(i) instanceof String) {
						rowPopulation.add(generateRandomString());
					} else if (rowPopulation.get(i) instanceof Timestamp) {
						rowPopulation.add(generateRandomDate());
					} else if (rowPopulation.get(i) instanceof Float) {
						rowPopulation.add(generateRandomfloat());
					}

					sb.append(rowPopulation.get(rowPopulation.size() - 1) + ",");
					rowPopulation.remove(rowPopulation.size() - 1);

				}
				synchronized (this) {
					sb.setLength(sb.length() - 1);
					pout.println(sb.toString());
					//pout.flush();
					sb.setLength(0);
					System.out.println("Thread "
							+ Thread.currentThread().getName()
							+ " has inserted " + rowsToBeinserted + " at "
							+ System.currentTimeMillis());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			 
		}
		
	}

	public static String generateRandomString() {
		final String AB = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
		SecureRandom rnd = new SecureRandom();
		StringBuilder sb = new StringBuilder(10);
		for (int i = 0; i < 10; i++)
			sb.append(AB.charAt(rnd.nextInt(AB.length())));
		return sb.toString();

	}

	public static Timestamp generateRandomDate(String startTime, String endTime) {
		long rangebegin = Timestamp.valueOf(startTime).getTime();
		long rangeend = Timestamp.valueOf(endTime).getTime();
		long diff = rangeend - rangebegin + 1;
		Timestamp randDate = new Timestamp(rangebegin
				+ (long) (Math.random() * diff));
		return randDate;

	}

	public static float generateRandomfloat() {
		float minX = 51.0f;
		float maxX = 251.0f;

		Random rand = new Random();

		float a = rand.nextFloat() * (maxX - minX) + minX;
		return a;

	}

	public synchronized static int generateUniqueID() {
		Random rand = new Random();
		int random_integer = rand.nextInt(100) + 1;
		return random_integer;
	}

	public synchronized static String generateUniqueEmail() {

		final String[] emailName = { "tim.harrison@email.si",
				"oren.weber@planet.tn", "del.pineda@shaw.ca",
				"seth.glenn@hotmail.com", "svetlana.grimes@gmail.com",
				"jerome.branch@gmail.com" };
		Random rand = new Random();
		int index = rand.nextInt(6);
		return emailName[index];
	}

	public static Timestamp generateRandomDate() {
		long rangebegin = Timestamp.valueOf("2017-02-01 00:00:00").getTime();
		long rangeend = Timestamp.valueOf("2017-02-09 00:00:00").getTime();
		long diff = rangeend - rangebegin + 1;
		Timestamp randDate = new Timestamp(rangebegin
				+ (long) (Math.random() * diff));
		SimpleDateFormat noMilliSecondsFormatter = new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss");
		System.out.println(noMilliSecondsFormatter.format(randDate));
		return randDate;

	}
}