package com.company.caesars.pgp;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.Security;

public class FileEncryptor {

	public static void main(String[] args) throws Exception {
		
		Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
	     
	     byte [] original = Decryptor.getBytesFromFile(new File("C:/Users/Micha³ Bluj/Desktop/marketing_lvm.ENT_P01_3263694_171213000008468001.gz"));
	     
	     FileInputStream pubKey = new FileInputStream("C://Users//Micha³ Bluj//Desktop//mcpublickey.key");
	     
	     byte[] encrypted = Decryptor.encrypt(original, Decryptor.readPublicKey(pubKey), null,
	                true, true);

	     FileOutputStream dfis = new FileOutputStream("C://Users//Micha³ Bluj//Desktop//heroku_marketing_lvm.ENT_P01_3263694_171213000008468001.gz.pgp");
	     dfis.write(encrypted);
	     dfis.close();
	}
	
}
