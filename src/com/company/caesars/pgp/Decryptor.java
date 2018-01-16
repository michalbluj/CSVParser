package com.company.caesars.pgp;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Security;
import java.util.Date;
import java.util.Iterator;

import org.bouncycastle.bcpg.ArmoredOutputStream;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openpgp.PGPCompressedData;
import org.bouncycastle.openpgp.PGPCompressedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedData;
import org.bouncycastle.openpgp.PGPEncryptedDataGenerator;
import org.bouncycastle.openpgp.PGPEncryptedDataList;
import org.bouncycastle.openpgp.PGPException;
import org.bouncycastle.openpgp.PGPLiteralData;
import org.bouncycastle.openpgp.PGPLiteralDataGenerator;
import org.bouncycastle.openpgp.PGPObjectFactory;
import org.bouncycastle.openpgp.PGPOnePassSignatureList;
import org.bouncycastle.openpgp.PGPPrivateKey;
import org.bouncycastle.openpgp.PGPPublicKey;
import org.bouncycastle.openpgp.PGPPublicKeyEncryptedData;
import org.bouncycastle.openpgp.PGPPublicKeyRing;
import org.bouncycastle.openpgp.PGPPublicKeyRingCollection;
import org.bouncycastle.openpgp.PGPSecretKey;
import org.bouncycastle.openpgp.PGPSecretKeyRingCollection;
import org.bouncycastle.openpgp.PGPUtil;

/**
 * Created by Michal Bluj on 2017-06-23.
 */
public class Decryptor {

    private static PGPPrivateKey findSecretKey(InputStream keyIn, long keyID, char[] pass) throws IOException, PGPException, NoSuchProviderException {
        PGPSecretKeyRingCollection pgpSec = new PGPSecretKeyRingCollection(org.bouncycastle.openpgp.PGPUtil.getDecoderStream(keyIn));
        PGPSecretKey pgpSecKey = pgpSec.getSecretKey(keyID);
        if (pgpSecKey == null) {
            return null;
        }
        return pgpSecKey.extractPrivateKey(pass, "BC");
    }

    @SuppressWarnings("unchecked")
    public static void decryptFile(InputStream in, OutputStream out, InputStream keyIn, char[] password)throws Exception{

        Security.addProvider(new BouncyCastleProvider());
        in = PGPUtil.getDecoderStream(in);
        PGPObjectFactory pgpF = new PGPObjectFactory(in);
        PGPEncryptedDataList enc;
        Object o = pgpF.nextObject();

        enc = o instanceof  PGPEncryptedDataList ? (PGPEncryptedDataList) o : (PGPEncryptedDataList) pgpF.nextObject();

        Iterator<PGPPublicKeyEncryptedData> it = enc.getEncryptedDataObjects();
        PGPPrivateKey sKey = null;
        PGPPublicKeyEncryptedData pbe = null;
        while (sKey == null && it.hasNext()) {
            pbe = it.next();
            sKey = findSecretKey(keyIn, pbe.getKeyID(), password);
        }
        if (sKey == null) {
            throw new IllegalArgumentException("Secret key for message not found.");
        }
        InputStream clear = pbe.getDataStream(sKey, "BC");
        PGPObjectFactory plainFact = new PGPObjectFactory(clear);
        Object message = plainFact.nextObject();
        if (message instanceof  PGPCompressedData) {
            PGPCompressedData cData = (PGPCompressedData) message;
            PGPObjectFactory pgpFact = new PGPObjectFactory(cData.getDataStream());
            message = pgpFact.nextObject();
        }
        if (message instanceof  PGPLiteralData) {
            PGPLiteralData ld = (PGPLiteralData) message;
            InputStream unc = ld.getInputStream();
            int ch;
            while ((ch = unc.read()) >= 0) {
                out.write(ch);
            }
        } else if (message instanceof  PGPOnePassSignatureList) {
            throw new PGPException("Encrypted message contains a signed message - not literal data.");
        } else {
            throw new PGPException("Message is not a simple encrypted file - type unknown.");
        }
        if (pbe.isIntegrityProtected()) {
            if (!pbe.verify()) {
                throw new PGPException("Message failed integrity check");
            }
        }
    }
    
    public static byte[] encrypt(byte[] clearData, PGPPublicKey encKey,
            String fileName,boolean withIntegrityCheck, boolean armor)
            throws IOException, PGPException, NoSuchProviderException {
        if (fileName == null) {
            fileName = PGPLiteralData.CONSOLE;
        }

        ByteArrayOutputStream encOut = new ByteArrayOutputStream();

        OutputStream out = encOut;
        if (armor) {
            out = new ArmoredOutputStream(out);
        }

        ByteArrayOutputStream bOut = new ByteArrayOutputStream();

        PGPCompressedDataGenerator comData = new PGPCompressedDataGenerator(
                PGPCompressedDataGenerator.ZIP);
        OutputStream cos = comData.open(bOut); // open it with the final
        // destination
        PGPLiteralDataGenerator lData = new PGPLiteralDataGenerator();

        // we want to generate compressed data. This might be a user option
        // later,
        // in which case we would pass in bOut.
        OutputStream pOut = lData.open(cos, // the compressed output stream
                PGPLiteralData.BINARY, fileName, // "filename" to store
                clearData.length, // length of clear data
                new Date() // current time
                );
        pOut.write(clearData);

        lData.close();
        comData.close();

        PGPEncryptedDataGenerator cPk = new PGPEncryptedDataGenerator(
                PGPEncryptedData.CAST5, withIntegrityCheck, new SecureRandom(),
                "BC");

        cPk.addMethod(encKey);

        byte[] bytes = bOut.toByteArray();

        OutputStream cOut = cPk.open(out, bytes.length);

        cOut.write(bytes); // obtain the actual bytes from the compressed stream

        cOut.close();

        out.close();

        return encOut.toByteArray();
    }

    public static PGPPublicKey readPublicKey(InputStream in)
            throws IOException, PGPException {
        in = PGPUtil.getDecoderStream(in);

        PGPPublicKeyRingCollection pgpPub = new PGPPublicKeyRingCollection(in);

        //
        // we just loop through the collection till we find a key suitable for
        // encryption, in the real
        // world you would probably want to be a bit smarter about this.
        //

        //
        // iterate through the key rings.
        //
        Iterator rIt = pgpPub.getKeyRings();

        while (rIt.hasNext()) {
            PGPPublicKeyRing kRing = (PGPPublicKeyRing) rIt.next();
            Iterator kIt = kRing.getPublicKeys();

            while (kIt.hasNext()) {
                PGPPublicKey k = (PGPPublicKey) kIt.next();

                if (k.isEncryptionKey()) {
                    return k;
                }
            }
        }

        throw new IllegalArgumentException(
                "Can't find encryption key in key ring.");
    }

    public static byte[] getBytesFromFile(File file) throws IOException {
        InputStream is = new FileInputStream(file);

        // Get the size of the file
        long length = file.length();

        if (length > Integer.MAX_VALUE) {
            // File is too large
        }

        // Create the byte array to hold the data
        byte[] bytes = new byte[(int)length];

        // Read in the bytes
        int offset = 0;
        int numRead = 0;
        while (offset < bytes.length
               && (numRead=is.read(bytes, offset, bytes.length-offset)) >= 0) {
            offset += numRead;
        }

        // Ensure all the bytes have been read in
        if (offset < bytes.length) {
            throw new IOException("Could not completely read file "+file.getName());
        }

        // Close the input stream and return bytes
        is.close();
        return bytes;
    }

}
