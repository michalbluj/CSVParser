package com.company.caesars.generator.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

public class CSVSplitter {
	
	//Users/blau/git/CSVParser/data/2017-07-19/marketing_campaigns.txt
	public static String baseDir = "data/2017-07-21";
	public static String inFile = "gst_activity_ftd.txt";
	public static String inFilePath = baseDir + "/" + inFile;
	public static String outFileBase = "gst_activity_ftd_";
	
	public static boolean scanOnly = false;
	public static int SPLITSIZE = 100000;
	
	public static void main(String[] args) throws IOException {

		InputStream in = new FileInputStream(inFilePath);		
		BufferedReader bReader = new BufferedReader(new InputStreamReader(in));
		
		String header = bReader.readLine();
		int fieldCount = countDelimiter(header,"\\|");
		System.out.println("Header has "+fieldCount+" fields");
		
		int count = 0;
		int lineCount = 0;
		int outFileCount = 1;
		String line;
		StringBuffer sbfFileContents = new StringBuffer();
		sbfFileContents.append(header).append("\r\n");
		lineCount++;
		
		while(  (line = bReader.readLine()) != null){
			lineCount++;
			/*
			if (lineCount==1752430) {
				System.out.println("start debugging");
			}
			*/
			
			int lineFieldCount = countDelimiter(line,"\\|") ;
			String nextLine=null;
			if (lineFieldCount != fieldCount) {
				System.err.println("linefield count("+lineFieldCount+") != header fieldcount("+fieldCount + ")\r\n"+line + "\r\n");
				while(lineFieldCount < fieldCount){
					nextLine =  bReader.readLine();
					line = addEncapsulator(line,nextLine);
					lineFieldCount = countDelimiter(line,"\\|") ;
				}
				if ( lineFieldCount != fieldCount) {
					System.err.println("lineCount - "+lineCount+ " lineFieldCount="+lineFieldCount +
						" cannot fix problem \r\n"+line+"\r\n");
					line = nextLine;
				} else {
					//double check
					int doubleCheckCount = countDelimiter(line,"\\|") ;
					if (doubleCheckCount !=fieldCount) {
						System.err.println("lineCount - "+lineCount+
							" failed Double Check field count is "+doubleCheckCount + "\r\n"+line + "\r\n");
					} else {
						System.out.println("problem fixed \r\n"+line+"\r\n");
					}
				}
				
				

			}
			sbfFileContents.append(line).append("\r\n");
			count++;
			
			if ( count == SPLITSIZE ) {
					
				String outFilePath=String.format("%s/%s_%02d.txt", 
						baseDir,outFileBase,outFileCount++);
				
				writeFile(outFilePath,sbfFileContents);
				sbfFileContents = new StringBuffer();
				sbfFileContents.append(header).append("\r\n");
				count = 0;
				
				//exit after 1 file
				//System.exit(0);
			}
		}
		
		//write the last partial file
		String outFilePath=String.format("%s/%s_%02d.txt", 
				baseDir,outFileBase,outFileCount++);	
		writeFile(outFilePath,sbfFileContents);
		
		System.out.println("file split operation completed");
		
		bReader.close();
		in.close();
		
	}
	
	private static String addEncapsulator(String line, String nextLine) {
		int index = line.lastIndexOf("|");
		String fixedLine = line.substring(0, index+1)+"\""+line.substring(index+1);
		String result = fixedLine+"\""+nextLine;;
		return result;
	}

	public static int countDelimiter(String line, String delimiter) {
		if (line == null) {
			return 0;
			//return new String[0];
		}
		String[] splitRes = line.split(delimiter);
		for(String field : splitRes) {
			if (field.contains("\r") || field.contains("\n")) {
				System.err.println("line contains End of Record \r\n"+
						line+"\r\n"+field+"\r\n");
			}
		}
		return splitRes.length;
	}
	
	public static void writeFile(String fileName, StringBuffer sbfFileContents) throws IOException {
		if (scanOnly) {
			System.out.println("ScanOnly skipping "+fileName);
		} else {
			System.out.println("creating "+fileName);
		}
		
		OutputStream out = new FileOutputStream(fileName);
		BufferedWriter bWriter = new BufferedWriter(new OutputStreamWriter(out));

		bWriter.write(sbfFileContents.toString());
		bWriter.close();
		out.close();
	}

}
