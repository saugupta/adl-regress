
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
import com.microsoft.azure.datalake.store.DirectoryEntry;
import com.microsoft.azure.datalake.store.IfExists;
import com.microsoft.azure.datalake.store.oauth2.AccessTokenProvider;
import com.microsoft.azure.datalake.store.oauth2.ClientCredsTokenProvider;

	public class ADLTests {
		private enum Store{
			ADL, LOCAL
		}
		private ADLConfig config; 
		private AccessTokenProvider provider;
	    private ADLStoreClient client;
	      public ADLTests(ADLConfig config){
	    	  	this.config=config;
	    	 	this.provider = new ClientCredsTokenProvider(config.getAuthTokenEndpoint(), config.getClientId(),config.getClientKey());
	    		this.client = ADLStoreClient.createClient(config.getAccountFQDN(), provider);  			 
	      }
		private void downloadFile(String pathLocal, String pathADL) throws Exception{
			long startTime=new Date().getTime();
			try{
				InputStream in = client.getReadStream(pathADL);
			
				if(!client.checkExists(pathADL)){
					List<DirectoryEntry> dE=client.enumerateDirectory(pathADL);
					throw new FileNotFoundException("File not found");
				}
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
		
		private void uploadFile(String pathLocal, String pathADL){
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
		
		private void cleanDirectory(Store store,String path){
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
	    public static void main(String[] args) throws Exception {
	    	if(args.length<7)
	    		throw new Exception("Please pass required Parameters: AccountFQDN\t"
	    				+ "TenantId\t"+"ClientId\t"+"ClientSecret\t"+ "downloadPathOnADL\t"+"uploadPathOnADL\t"+ "pathLocal");
	    	ADLTests tests= new ADLTests(new ADLConfig().setAccountName(args[0]).setTenantId(args[1]).setClientId(args[2]).setClientKey(args[3]).build());
	    	//String pathLocal="/home/saugupta/Downloads/agent64_install_ankur.bin";
	    	String downloadPathADL=args[4];
	    	String uploadPathADL=args[5];
	    	String pathLocal=args[6];
	    	tests.downloadFile(pathLocal,downloadPathADL);
	    	tests.uploadFile(pathLocal,uploadPathADL);
	    	tests.cleanDirectory(Store.LOCAL,pathLocal);
	    	tests.cleanDirectory(Store.ADL,uploadPathADL);
	    }
}