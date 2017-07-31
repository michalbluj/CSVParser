package com.company.caesars.pgp;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

//Decryptor *pgp files in a directory
public class DirDecryptor {

	public static void main(String[] args) throws IOException {
		
		//setup basic decryption criteria
		InputStream keyIn = new FileInputStream("keys/PGP/st/private.asc");
		char[] password = "Ri0Vegas!".toCharArray();
		
		///blauHome/Caesars/data/GuestPreference/gst_pref_events.zip.pgp
		String inFile = "data/GuestPreference/gst_pref_events.zip.pgp";
		
		decryptFile(inFile, getOutFile(inFile), keyIn, password);
	}
	
	public static String getOutFile(String inFile) {
		if (inFile.endsWith(".pgp") || inFile.endsWith(".PGP")) {
			return inFile.substring(0, inFile.length()-4);
		} else {
			return null;
		}
	}
	
	public static void decryptFile(String inFile, String outFile, InputStream keyIn, char[] password) throws IOException {
		System.out.println("decryptFile - "+ inFile);
		InputStream in = new FileInputStream(inFile);
		OutputStream out = new FileOutputStream(outFile);
			
		try {
			Decryptor.decryptFile(in, out, keyIn, password);
		} catch (Exception e) {
			System.err.println("decryptFile Exception from "+ inFile);
			e.printStackTrace();
		}
		
		out.close();
		System.out.println("decryptFile - "+ inFile+" completed");
	}
}
