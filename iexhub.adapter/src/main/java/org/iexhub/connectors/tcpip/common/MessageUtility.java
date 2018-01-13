
package org.iexhub.connectors.tcpip.common;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.math.BigInteger;
import java.security.InvalidKeyException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HashMap;
import java.util.Iterator;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.spec.SecretKeySpec;

import org.apache.log4j.Logger;

import com.eversolve.Medi7.M7DefinitionFile;
import com.eversolve.Medi7.M7Message;

/*******************************************************************************
 * Copyright (c) 2015, 2017  Veterans Health Administration
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at     http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contributors:
 *     Ioana Singureanu Eversolve, LLC, IHE NA Connectathon 2017 Version 
 *******************************************************************************/
public class MessageUtility {
	/**
	 * File logger of type 'Logger'
	 */
	static Logger logger = Logger.getLogger(MessageUtility.class);

	static String CUSTOM_VALIDATION_EQUALS = "/properties/message/custom_validation/equals";

	/**
	 * Encryption ALGORITHM of type 'String'
	 */
	public static String ALGORITHM = ConnectorProperties.getInstance()
			.getProperty("/properties/message/encryption/algorithm");

	/**
	 * Key HASH algorithm of type 'String'
	 */
	static String HASH = ConnectorProperties.getInstance().getProperty("/properties/message/encryption/hash");

	/**
	 * Key PASSWORD of type 'String'
	 */
	public static String PASSWORD = ConnectorProperties.getInstance()
			.getProperty("/properties/message/encryption/password");

	/**
	 * Message definition file <i>mdf</i> of type 'M7DefinitionFile'
	 */
	M7DefinitionFile mdf;

	public MessageUtility() {

		String defFileName = ConnectorProperties.getInstance().getProperty("/properties/message/definition");
		initialize(defFileName);
	}

	/**
	 * Constructor
	 * 
	 * @param defFileName
	 */
	public MessageUtility(String defFileName) {
		initialize(defFileName);
	}

	/**
	 * Constructor
	 * 
	 * @param defFile
	 */
	public MessageUtility(M7DefinitionFile defFileName) {
		this.setMdf(defFileName);
	}

	/**
	 * initialize
	 * 
	 * @param defFileName
	 */
	protected void initialize(String defFileName) {
		try {

			mdf = new M7DefinitionFile(defFileName);
		} catch (Exception e1) {
			logger.error(e1);
			e1.printStackTrace();
			System.exit(0);
		}
	}

	/**
	 * getClearText
	 * 
	 * @param cryptoText
	 * @param length
	 * @param password
	 * @return decrypted byte array
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws BadPaddingException
	 * @throws IllegalBlockSizeException
	 */
	public static byte[] decryptText(byte[] cryptoText, int length)
			throws NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
		Cipher c = null;
		logger.debug("Encrypted text:" + new String(cryptoText));
		try {
			c = Cipher.getInstance(ALGORITHM);
		} catch (Exception e) {
			logger.error("Encryption initialization error '" + e.getLocalizedMessage() + "'");
			e.printStackTrace();
			System.exit(0);
		}
		SecretKeySpec keySpec = null;
		keySpec = new SecretKeySpec(getHash(), 0, 16, ALGORITHM);
		byte[] decrypted = null;
		c.init(Cipher.DECRYPT_MODE, keySpec);
		decrypted = c.doFinal(cryptoText, 0, length);
		logger.debug("Decoded message:" + new String(decrypted));
		return decrypted;
	}

	/**
	 * getEncryptedText
	 * 
	 * @param clearText
	 * @param length
	 * @param password
	 * @return
	 * @throws NoSuchAlgorithmException
	 * @throws InvalidKeyException
	 * @throws IllegalBlockSizeException
	 * @throws BadPaddingException
	 */
	public static byte[] encryptText(byte[] clearText, int length)
			throws NoSuchAlgorithmException, InvalidKeyException, IllegalBlockSizeException, BadPaddingException {
		Cipher c = null;

		try {
			c = Cipher.getInstance(ALGORITHM);
		} catch (Exception e) {
			logger.error("Encryption initialization error '" + e.getLocalizedMessage() + "'");
			e.printStackTrace();
			System.exit(0);
		}
		SecretKeySpec keySpec = null;
		keySpec = new SecretKeySpec(getHash(), 0, 16, ALGORITHM);
		byte[] encrypted = null;
		c.init(Cipher.ENCRYPT_MODE, keySpec);
		logger.debug("Clear message:\n" + new String(clearText));
		encrypted = c.doFinal(clearText, 0, length);
		logger.debug("Encoded message:\n" + new String(encrypted));
		return encrypted;
	}

	/**
	 * getHash
	 * 
	 * @param password
	 * @return
	 * @throws NoSuchAlgorithmException
	 */
	private static byte[] getHash() throws NoSuchAlgorithmException {
		logger.debug("password:" + PASSWORD + ".");
		MessageDigest messageDigest;
		byte[] l_bytes = null;
		String hPassword = "";
		BigInteger hash = null;

		messageDigest = MessageDigest.getInstance(HASH);
		messageDigest.update(PASSWORD.getBytes(), 0, PASSWORD.length());
		l_bytes = messageDigest.digest();
		hash = new BigInteger(1, l_bytes);

		logger.debug("hash :" + hash);
		hPassword = hash.toString(16);
		logger.debug(hPassword);
		return hash.toByteArray();
	}

	/**
	 * <b>validateMessage</b> provides custom testing and prints the required
	 * fields and errors encountered in a message instance.
	 * 
	 * @param message
	 */
	public boolean validateMessage(M7Message message, StringBuffer errorStr) {

		boolean success = true;
		HashMap<String, String> validationMap = ConnectorProperties.getInstance()
				.getLookupTables(CUSTOM_VALIDATION_EQUALS);
		Iterator<String> fieldsToValidateCollection = validationMap.keySet().iterator();
		while (fieldsToValidateCollection.hasNext()) {
			String fieldPath = fieldsToValidateCollection.next();
			String value = validationMap.get(fieldPath);
			String fieldValue = null;
			try {
				fieldValue = message.getFieldValue(fieldPath);
			} catch (Exception e) {
				errorStr.append("Invalid field path: " + fieldPath);
				logger.error(errorStr.toString() + "in:\n" + message.debug());
				success = false;
			}
			if (fieldValue != null) {
				if (fieldValue.equalsIgnoreCase(value)) {
					logger.debug("Field " + fieldPath + " = '" + fieldValue + "' matches '" + value + "'");
				} else {
					errorStr.append("Field " + fieldPath + " = '" + fieldValue + "' does not match the expected '"
							+ value + "'");
					logger.debug(errorStr);
					success = false;

				}
			}
			else
			{
				errorStr.append("Null value at " + fieldPath+ "; does not match '"+value+"'");
				success = false;
			}

		}
		/*
		 * StringBuffer outputStr = new StringBuffer(); outputStr.append("\n\n"
		 * + message + "\n---------- \n"); try { outputStr.append("Device id: "
		 * + message.getFieldValue("ORC.EnteringDevice.identifier") + "\n"); }
		 * catch (M7Exception e3) { // TODO Auto-generated catch block //
		 * e3.printStackTrace(); errorStr.append("Missing device id\n");
		 * outputStr.append("Device id: ?\n"); } String resultType = null; try {
		 * resultType = message
		 * .getFieldValue("OBR.UniversalServiceIdentif.identifier");
		 * errorStr.append("Result type: " + resultType + "\n"); } catch
		 * (M7Exception e3) { // TODO Auto-generated catch block //
		 * e3.printStackTrace(); errorStr.append("Missing Result type\n");
		 * outputStr.append("Result type: ? \n"); } try { outputStr.append(
		 * "Patient id: " +
		 * message.getFieldValue("PID.PatientIdentifierList.ID") + "\n"); }
		 * catch (M7Exception e1) {
		 * 
		 * // check the bed number instead try { outputStr
		 * 
		 * .append("Bed id: " + message
		 * .getFieldValue("PV1.AssignedPatientLocation.bed") + "\n"); } catch
		 * (M7Exception e) { // TODO Auto-generated catch block //
		 * e1.printStackTrace(); errorStr.append(
		 * "Missing patient id and bed number\n"); outputStr.append(
		 * "Patient/Bed id: ?\n");
		 * 
		 * } } try { outputStr.append("Patient type: " +
		 * message.getFieldValue("PV1.PatientType") + "\n"); } catch
		 * (M7Exception e2) { // TODO Auto-generated catch block //
		 * e2.printStackTrace(); errorStr.append("Missing patient type\n");
		 * outputStr.append("Patient type: ?\n"); } try { M7DateTime dtm =
		 * message
		 * .getFieldValueAsDateTime("OBR.ObservationDateTime#.DateTime");
		 * com.eversolve.Medi7.datatypes.M7Date dt = dtm.getDate(); String month
		 * = Integer.toString(dt.getMonth()); if (dt.getMonth() < 10) { month =
		 * "0" + month;
		 * 
		 * } String day = Integer.toString(dt.getDay()); if (dt.getDay() < 10) {
		 * day = "0" + day; } String date = month + "/" + day + "/" +
		 * Integer.toString(dt.getYear()).substring(2, 4); M7Time tm =
		 * dtm.getTime();
		 * 
		 * String meridian = "AM";
		 * 
		 * int hour = tm.getHours(); String hoursStr = Integer.toString(hour);
		 * if (hour >= 12) { meridian = "PM"; if (hour > 12) { hour = hour % 12;
		 * } hoursStr = Integer.toString(hour); } if (hour < 10) { hoursStr =
		 * "0" + hoursStr; } String min = Integer.toString(tm.getMinutes()); if
		 * (tm.getMinutes() < 10) { min = "0" + min; }
		 * 
		 * String sec = Integer.toString(tm.getSeconds()); if (tm.getSeconds() <
		 * 10) { sec = "0" + sec; } outputStr.append("Date/time: " + date + " "
		 * + hoursStr + ":" + min + ":" + sec + " " + meridian + "\n"); } catch
		 * (M7Exception e3) { // TODO Auto-generated catch block //
		 * e3.printStackTrace(); outputStr.append("Date/time: ?\n");
		 * errorStr.append("Missing date/time\n"); } M7Repeat observations =
		 * null; try {
		 * 
		 * observations = message.getRepeat("OBX"); } catch (M7Exception e4) {
		 * logger.error(e4); e4.printStackTrace(); } M7Composite seg = null;
		 * 
		 * if (observations == null) { errorStr.append("Missing Observations\n"
		 * ); } else { for (long i = 0; ((observations != null) && (i <
		 * observations .getSize())); i++) { try { seg =
		 * observations.getChildByPosition(i); } catch (M7Exception e5) {
		 * errorStr.append("Invalid observation information\n"); seg = null; }
		 * if (seg != null) { String testid = "?"; try { testid = seg
		 * .getFieldValue("ObservationIdentifier.text") .trim();
		 * 
		 * } catch (M7Exception e7) { // stays '?' } outputStr.append(" " +
		 * testid); for (int n = 0; n < (15 - testid.length()); n++) { // pad
		 * with spaces outputStr.append("."); } String flag = "";
		 * outputStr.append("= "); String result = "-?-"; try { result =
		 * seg.getFieldValue("ObservationValue"); } catch (M7Exception e8) { //
		 * check the result status try { flag =
		 * seg.getFieldValue("ObservationResultStatus"); } catch (M7Exception e)
		 * { errorStr.append("Missing result status flag\n"); } // if
		 * (flag.length() == 0 || !flag.equals("X")) { //
		 * outputStr.append("-?-"); // } else if (flag.equals("X")) { //
		 * outputStr.append("-?-"); // } } outputStr.append(result); String
		 * codeset = ""; try { outputStr.append(" " +
		 * seg.getFieldValue("Units.text")); codeset = seg
		 * .getFieldValue("ObservationIdentifier.nameofcodingsystem"); } catch
		 * (M7Exception e10) { if ((resultType != null) &&
		 * (resultType.equalsIgnoreCase("ALARM"))) { // } else if
		 * (codeset.equalsIgnoreCase("MDIL-ALERT")) { } else if
		 * (flag.equalsIgnoreCase("X")) { } else { outputStr.append(" ?");
		 * errorStr.append("Missing units of measure\n"); } } String aFlag = "";
		 * try { aFlag = seg.getFieldValue("AbnormalFlags"); } catch
		 * (M7Exception e10) { } if (aFlag.equals("H")) { outputStr.append(
		 * " (HIGH)"); } else if (aFlag.equals("L")) { outputStr.append(" (LOW)"
		 * ); } else if (aFlag.equals("A")) { outputStr.append(" (ABNORMAL) ");
		 * } else if (aFlag.length() > 0) { outputStr.append(" ABNORMAL FLAG '"
		 * + aFlag + "'"); errorStr.append("Unknown abnormal flag '" + aFlag +
		 * "'\n"); } if (testid.equalsIgnoreCase("HR")) { String method = "";
		 * try { method = seg .getFieldValue("ObservationMethod.text"); } catch
		 * (M7Exception e10) { if (!result.equals("-?-")) {
		 * 
		 * errorStr .append("Missing observation method for HR\n"); }
		 * outputStr.append(" (?)"); } if (method.length() > 0) {
		 * outputStr.append(" (" + method + ")"); } } outputStr.append("\n"); }
		 * } }
		 */
		if (errorStr.length() > 0) {
			logger.info(" Errors: \n" + errorStr.toString());
			return false;
		} else {
			return success;
		}

	}

	/**
	 * @param inputStr
	 * @return String without whitespace
	 */
	public static String removeWhiteSpace(String inputStr) {
		StringBuffer xmlStringNoWhiteSpace = new StringBuffer();
		int i = 0;
		while (i < inputStr.length()) {
			char current = inputStr.charAt(i);
			if ((!Character.isWhitespace(current)) || Character.isSpaceChar(current)) {
				xmlStringNoWhiteSpace.append(inputStr.charAt(i));
			}
			i++;

		}

		return xmlStringNoWhiteSpace.toString();
	}

	/**
	 * @param sMessage
	 */
	public static void printMessage(String sMessage) {
		StringBuffer sb = new StringBuffer(sMessage);
		for (int i = 0; i < sb.length(); i++) {
			if (sb.charAt(i) == '\r')
				sb.setCharAt(i, '\n');
		}

		System.out.println(sb);
	}

	/**
	 * @param stream
	 * @return
	 * @throws IOException
	 */
	public static String getString(InputStream stream) throws IOException {
		BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
		StringBuffer buffer = new StringBuffer();
		String line;

		while ((line = reader.readLine()) != null) {
			buffer.append(line);
			buffer.append(M7Message.MSG_TERMINATE_CHAR);
		}
		return buffer.toString();
	}

	/**
	 * @return
	 */
	public M7DefinitionFile getMdf() {
		return mdf;
	}

	/**
	 * @param messageDef
	 */
	public void setMdf(M7DefinitionFile mdf) {
		this.mdf = mdf;
	}

	/**
	 * readFile - used to read a test file if the Adapter is used to simulate a
	 * sending system
	 * 
	 * @param filePath
	 *            Input file path
	 * @param charsetName
	 *            UTF-8
	 * @return
	 * @throws IOException
	 */
	public static String readFile(String filePath, String charsetName) throws IOException {
		// Java 7 impl using Charset.UTF_8ss
		// byte[] encoded = Files.readAllBytes(Paths.get(path));
		// return new String(encoded, encoding);
		java.io.InputStream is = new java.io.FileInputStream(filePath);
		try {
			final int bufsize = 4096;
			int available = is.available();
			byte[] data = new byte[available < bufsize ? bufsize : available];
			int used = 0;
			while (true) {
				if (data.length - used < bufsize) {
					byte[] newData = new byte[data.length << 1];
					System.arraycopy(data, 0, newData, 0, used);
					data = newData;
				}
				int got = is.read(data, used, data.length - used);
				if (got <= 0)
					break;
				used += got;
			}
			return charsetName != null ? new String(data, 0, used, charsetName) : new String(data, 0, used);
		} finally {
			is.close();
		}

	}

}