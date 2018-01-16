package com.company.caesars.pgp;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileDecryptor {

	public static void main(String[] args) throws IOException {

		///blauHome/Caesars/data/GuestPreference/gst_tier.zip.pgp
		InputStream in = new FileInputStream("C:/Users/Micha³ Bluj/Desktop/prop_desc_sda.ENT_ALL_103_171211000008410002.gz.pgp");
		OutputStream out = new FileOutputStream("C:/Users/Micha³ Bluj/Desktop/prop_desc_sda.ENT_ALL_103_171211000008410002.gz");
		
		
		InputStream keyIn = new FileInputStream("keys/PGP/st/private.asc");
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
