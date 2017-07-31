package com.company.caesars.pgp;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileDecryptor {

	public static void main(String[] args) throws IOException {

		///blauHome/Caesars/data/GuestPreference/gst_tier.zip.pgp
		InputStream in = new FileInputStream("data/2017-07-21/marketing_lvm.zip.pgp");
		OutputStream out = new FileOutputStream("data/2017-07-21/marketing_lvm.zip");
		
		System.out.println("Decrypting "+"data/2017-07-21/marketing_lvm.zip.pgp");
		InputStream keyIn = new FileInputStream("data/sample/private.asc");
		//InputStream keyIn = new FileInputStream("keys/PGP/devst/privateDev.asc");
		
		char[] password = "Ri0Vegas!".toCharArray();	
		//char[] password = "L1nqVegas!".toCharArray();	
		
		try {
			Decryptor.decryptFile(in, out, keyIn, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		out.close();   
		
		System.out.println("FileDecryptor completed.");
	}
}
