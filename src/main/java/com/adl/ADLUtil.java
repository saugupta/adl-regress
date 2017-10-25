package com.adl;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.util.Date;
import java.util.List;

import com.microsoft.azure.datalake.store.ADLStoreClient;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;


public class ADLUtil {
	public static enum Store{
		ADL, LOCAL
	}

	void downloadFile(ADLConfig config, String pathLocal, String pathADL) throws Exception{
		AccessTokenProvider provider = new ClientCredsTokenProvider(config.getAuthTokenEndpoint(), config.getClientId(),config.getClientKey());
		 ADLStoreClient client = ADLStoreClient.createClient(config.getAccountFQDN(), provider);  			 
	
		long startTime=new Date().getTime();
		try{
		
			if(!client.checkExists(pathADL)){
				throw new FileNotFoundException("File not found");
			}
			InputStream in = client.getReadStream(pathADL);
			BufferedReader reader = new BufferedReader(new InputStreamReader(in));
			String line;
			PrintStream writer = new PrintStream(pathLocal);
			while ( (line = reader.readLine()) != null) {
			    writer.println(line);
			}
			writer.close();
			reader.close();
	    	double endTime=new Date().getTime();
	    	double timeTaken=(endTime-startTime)/1000; // In seconds
	    	double fileSize=new File(pathLocal).length()/(1024*1024);
	     	System.out.println("Total Time taken:"+timeTaken+ "for Size:"+fileSize  +"MB. Download Speed:"+((fileSize)/(timeTaken+1))+ " MB/Sec");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	 void uploadFile(ADLConfig config, String pathLocal, String pathADL){
		 AccessTokenProvider provider = new ClientCredsTokenProvider(config.getAuthTokenEndpoint(), config.getClientId(),config.getClientKey());
		 ADLStoreClient client = ADLStoreClient.createClient(config.getAccountFQDN(), provider);  			 
		 long startTime=new Date().getTime();
		try{
			FileReader fr= new FileReader(pathLocal);
			BufferedReader br= new BufferedReader(fr);
			client.createDirectory(pathADL.substring(0, pathADL.lastIndexOf('/')-1));
			             //create file and write some content
			            String filename = pathADL;
			            OutputStream stream = client.createFile(filename, IfExists.OVERWRITE  );
			            PrintStream out = new PrintStream(stream);
			            String sCurrentLine;
			            while((sCurrentLine=br.readLine())!=null)
			                out.println(sCurrentLine);
			           out.close();
				    	double endTime=new Date().getTime();
				    	double timeTaken=(endTime-startTime)/1000; // In seconds
				    	double fileSize=new File(pathLocal).length()/(1024*1024);
				     	System.out.println("Total Time taken:"+timeTaken+ "for Size:"+fileSize  +"MB. Upload Speed:"+((fileSize)/(timeTaken+1))+ " MB/Sec");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	 void cleanDirectory(ADLConfig config, Store store,String path){
		 AccessTokenProvider provider = new ClientCredsTokenProvider(config.getAuthTokenEndpoint(), config.getClientId(),config.getClientKey());
		 ADLStoreClient client = ADLStoreClient.createClient(config.getAccountFQDN(), provider);  			 
		 try {
			switch(store){
			case ADL:
				client.deleteRecursive(path);
				break;
			case LOCAL:
				Files.delete(FileSystems.getDefault().getPath(path));
			}
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	 void uploadMultipleFilesToSameOutputFile(ADLConfig config, List<String> pathLocals, String pathADL){ 
		 AccessTokenProvider provider = new ClientCredsTokenProvider(config.getAuthTokenEndpoint(), config.getClientId(),config.getClientKey());
		 ADLStoreClient client = ADLStoreClient.createClient(config.getAccountFQDN(), provider);  			 
	
		 long startTime=new Date().getTime();
		try{
			client.createDirectory(pathADL.substring(0, pathADL.lastIndexOf('/')-1));
			String filename = pathADL;
			//create file and write some content
		 	OutputStream stream = client.createFile(filename, IfExists.OVERWRITE );				
		 	PrintStream out = new PrintStream(stream);
		 	double fileSize=0;		 	
			for(String path: pathLocals){
					FileReader fr= new FileReader(path);
					BufferedReader br= new BufferedReader(fr);
			        String sCurrentLine;
			        while((sCurrentLine=br.readLine())!=null){
			                out.println(sCurrentLine);
			        }
			        fileSize+=new File(path).length()/(1024*1024);
			}
			out.close();
	    	double endTime=new Date().getTime();
	    	double timeTaken=(endTime-startTime)/1000; // In seconds
	     	System.out.println("Total Time taken:"+timeTaken+ "for Size:"+fileSize  +"MB. Upload Speed:"+((fileSize)/(timeTaken+1))+ " MB/Sec");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	 void adlToadl(String pathADL1,ADLConfig config1,String pathADL2, ADLConfig config2){
		 AccessTokenProvider provider1 = new ClientCredsTokenProvider(config1.getAuthTokenEndpoint(), config1.getClientId(),config1.getClientKey());
		 ADLStoreClient client1 = ADLStoreClient.createClient(config1.getAccountFQDN(), provider1);  			 
		 AccessTokenProvider provider2 = new ClientCredsTokenProvider(config2.getAuthTokenEndpoint(), config2.getClientId(),config2.getClientKey());
		 ADLStoreClient client2  = ADLStoreClient.createClient(config2.getAccountFQDN(), provider2);  			 
		 long startTime=new Date().getTime();
			try{
				if(!client1.checkExists(pathADL1)){
					throw new FileNotFoundException("File not found");
				}
				InputStream in = client1.getReadStream(pathADL1);
				BufferedReader reader = new BufferedReader(new InputStreamReader(in));
				
				client2.createDirectory(pathADL2.substring(0, pathADL2.lastIndexOf('/')-1));
				//create file and write some content
			 	OutputStream stream = client2.createFile(pathADL2, IfExists.OVERWRITE );				
			 	PrintStream out = new PrintStream(stream);
				
			    String sCurrentLine;
		        while((sCurrentLine=reader.readLine())!=null){
		                out.println(sCurrentLine);
		          //    System.out.println(sCurrentLine);
		        }
				out.close();
				in.close();
		    	double endTime=new Date().getTime();
		    	double timeTaken=(endTime-startTime)/1000; // In seconds
		     	System.out.println("Total Time taken:"+timeTaken);
			} catch (IOException e) {
				e.printStackTrace();
			}
	 }
}
