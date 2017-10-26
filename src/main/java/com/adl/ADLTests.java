package com.adl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import com.adl.ADLUtil.Store;

public class ADLTests {
		private enum TASK{
			DowloadADL(0),UploadToADL(1),ADLtoADLCopy(2),DownloadUploadADLThroughput(3), UploadFileWithSize(4);
			private final int value;

			TASK(final int newValue) {
	            value = newValue;
	        }

	        public int getValue() { return value; }
		}
		private static void DownloadUploadADLThroughput(List<String> args) throws Exception{
			if(args.size()<7)
				throw new Exception("Please pass required Parameters:"
						+ " AccountFQDN\t" + "TenantId\t"+"ClientId\t"+"ClientSecret\t"
						+ "downloadPathOnADL\t"+"uploadPathOnADL\t"+ "pathLocal");
			ADLConfig config = new ADLConfig().setAccountName(args.get(0)).setTenantId(args.get(1)).setClientId(args.get(2)).setClientKey(args.get(3)).build();
			ADLUtil adlUtil= new ADLUtil();
			String downloadPathADL=args.get(4);
			String uploadPathADL=args.get(5);
			String pathLocal=args.get(6);
			adlUtil.downloadFile(config,pathLocal,downloadPathADL);
			adlUtil.uploadFile(config, pathLocal, uploadPathADL);
			adlUtil.cleanDirectory(config,Store.LOCAL,pathLocal);
			adlUtil.cleanDirectory(config,Store.ADL,uploadPathADL);
		}
		
		private static void DowloadADL(List<String> args) throws Exception{
			if(args.size()<6)
				throw new Exception("Please pass required Parameters:"
						+ " AccountFQDN\t"+ "TenantId\t"+"ClientId\t"+"ClientSecret\t"+"downloadPathOnADL\t"+ "pathLocal\t");
			ADLConfig config = new ADLConfig().setAccountName(args.get(0)).setTenantId(args.get(1)).setClientId(args.get(2)).setClientKey(args.get(3)).build();
				ADLUtil adlUtil= new ADLUtil();
			String downloadPathOnADL=args.get(4);
			String pathLocal=args.get(5);
			adlUtil.downloadFile(config, pathLocal, downloadPathOnADL);
		}
		
		private static void UploadToADL(List<String> args) throws Exception{
			if(args.size()<7)
				throw new Exception("Please pass required Parameters:"
						+ " AccountFQDN\t"+ "TenantId\t"+"ClientId\t"+"ClientSecret\t"+"uploadPathOnADL\t"+ "pathLocal\t"+ "factor");
			ADLConfig config = new ADLConfig().setAccountName(args.get(0)).setTenantId(args.get(1)).setClientId(args.get(2)).setClientKey(args.get(3)).build();
			ADLUtil adlUtil= new ADLUtil();
			String uploadPathADL=args.get(4);
			String pathLocal=args.get(5);
			// Total args[7] copies of input data copied to ADL
			List<String> pathLocals= new ArrayList<String>();
			pathLocals.addAll(Collections.nCopies(Integer.parseInt(args.get(6)),pathLocal));
			adlUtil.uploadMultipleFilesToSameOutputFile(config,pathLocals, uploadPathADL);
		}
		
		private static void UploadFileWithGivenSize(List<String> args) throws Exception{
			if(args.size()<7)
				throw new Exception("Please pass required Parameters:"
						+ " AccountFQDN\t"+ "TenantId\t"+"ClientId\t"+"ClientSecret\t"+"uploadPathOnADL\t"+ "pathLocalFolder\t"+ "SizeInGB");
			ADLConfig config = new ADLConfig().setAccountName(args.get(0)).setTenantId(args.get(1)).setClientId(args.get(2)).setClientKey(args.get(3)).build();
			ADLUtil adlUtil= new ADLUtil();
			String uploadPathADL=args.get(4);
			String pathLocalFolder=args.get(5);
			// Use 8 GB file and 2 GB File to generate load.
			// Total args[6] copies of input data copied to ADL
			List<String> pathLocals= new ArrayList<String>();
			int totalSize=Integer.parseInt(args.get(6));
			pathLocals.addAll(Collections.nCopies(totalSize/8,pathLocalFolder+"dataFile8GB.csv"));
			adlUtil.uploadMultipleFilesToSameOutputFile(config,pathLocals, uploadPathADL);
		}
		
		private static void ADLtoADLCopy(List<String> args) throws Exception{
			if(args.size()<10)
	    		throw new Exception("Please pass required Parameters:"
	    				+ " AccountFQDN1\t"+ "TenantId1\t"+"ClientId1\t"+"ClientSecret1\t"
	    				+ "AccountFQDN2\t" + "TenantId2\t"+"ClientId2\t"+"ClientSecret2\t" 
	    				+"downloadPathOnADL\t"+"uploadPathOnADL\t");
	    	ADLUtil adlUtil = new ADLUtil();
	    	ADLConfig config1 = new ADLConfig().setAccountName(args.get(0)).setTenantId(args.get(1)).setClientId(args.get(2)).setClientKey(args.get(3)).build();
	    	ADLConfig config2 = new ADLConfig().setAccountName(args.get(4)).setTenantId(args.get(5)).setClientId(args.get(6)).setClientKey(args.get(7)).build();
			
	    	String pathADL1=args.get(8);
	    	String pathADL2=args.get(9);
	    	
	    	adlUtil.adlToadl(pathADL1, config1, pathADL2, config2);
		}
		
		
	    public static void main(String[] args) throws Exception { 	
	    	System.out.println("Arguments passed:");
	    	for(int i=0;i<args.length;i++){
				System.out.println(args[i]);
			}
			if(args.length<1)
				System.out.println("Please select a task to do:");
			TASK taskChoosed=TASK.valueOf(args[0]);
			List<String> params=Arrays.asList(args).subList(1,args.length);
			switch(taskChoosed){
			case DowloadADL:
				DowloadADL(params);
				break;
			case UploadToADL:
				UploadToADL(params);
				break;
			case UploadFileWithSize:
				UploadFileWithGivenSize(params);
				break;
			case ADLtoADLCopy:
				ADLtoADLCopy(params);
				break;
			case DownloadUploadADLThroughput:
				DownloadUploadADLThroughput(params);
				break;
			}
			System.out.println("Task Ended");
	    }
}