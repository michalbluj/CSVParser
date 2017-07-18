package com.company.caesars.pgp;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class FileDecryptor {

	public static void main(String[] args) throws IOException {
		
		InputStream in = new FileInputStream("test_file3.txt.pgp");
		OutputStream out = new FileOutputStream("test_file3.txt");
		InputStream keyIn = new FileInputStream("private.asc");
		char[] password = "Ri0Vegas!".toCharArray();	
		
		try {
			Decryptor.decryptFile(in, out, keyIn, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		out.close();
	}
}
