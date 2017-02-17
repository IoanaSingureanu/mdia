
package org.iexhub.connectors.tcpip;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.net.Socket;
import com.eversolve.Medi7.M7DefinitionFile;
import com.eversolve.Medi7.M7Exception;
import com.eversolve.Medi7.M7Field;
import com.eversolve.Medi7.M7Message;
import com.eversolve.Medi7.datatypes.M7DateTime;
import com.eversolve.Medi7.util.M7StringHolder;

import org.apache.log4j.Logger;
import org.iexhub.connectors.database.DatabaseMap;
import org.iexhub.connectors.tcpip.common.ConnectorProperties;
import org.iexhub.connectors.tcpip.common.MessageUtility;
import org.iexhub.connectors.tcpip.common.localization.Messages;

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

public class ReceiverThread extends Thread {

	static Logger logger = Logger.getLogger(ReceiverThread.class);

	static boolean VALIDATION = ConnectorProperties.getInstance().getProperty("/properties/message/validation")
			.equalsIgnoreCase("true");
	static boolean PERSISTENCE = ConnectorProperties.getInstance().getProperty("/properties/message/persistence")
			.equalsIgnoreCase("true");
	static boolean CUSTOM_VALIDATION = ConnectorProperties.getInstance()
			.getProperty("/properties/message/custom_validation/@value").equalsIgnoreCase("true");
	static boolean ENCRYPTION = ConnectorProperties.getInstance().getProperty("/properties/message/encryption/enabled")
			.equalsIgnoreCase("true");
	// default
	static String ACK_CODE = ConnectorProperties.getInstance().getProperty("/properties/ack/code");
	// success
	static String ACK_CODE_SUCCESS = ConnectorProperties.getInstance().getProperty("/properties/ack/code/@success");
	// failure
	static String ACK_CODE_ERROR = ConnectorProperties.getInstance().getProperty("/properties/ack/code/@error");
	static boolean RETURN_ACK = ConnectorProperties.getInstance().getProperty("/properties/ack/mode")
			.equalsIgnoreCase("AL");
	static String ENCODING = ConnectorProperties.getInstance().getProperty("/properties/message/encoding");
	static String INITIATING_MSG_DEF = ConnectorProperties.getInstance().getProperty("/properties/message/definition");
	static String INITIATING_PROFILE = ConnectorProperties.getInstance().getProperty("/properties/message/profile");
	static String ACK_TYPE = ConnectorProperties.getInstance().getProperty("/properties/message/ack/type");
	static String ACK_PROFILE = ConnectorProperties.getInstance().getProperty("/properties/message/ack/profile");
	static String ACK_MSG_DEF = ConnectorProperties.getInstance().getProperty("/properties/message/ack/file");
	static String ADAPTER_NAME = ConnectorProperties.getInstance().getProperty("/properties/adapter/name");

	/**
	 * clientSocket of type 'Socket'
	 */
	private Socket clientSocket = null;

	/**
	 * out of type 'PrintWriter'
	 */
	private PrintWriter outWriter = null;

	/**
	 * in of type 'BufferedReader' for reading character data/HL7
	 */
	private BufferedReader inReader = null;

	/**
	 * inStream of type 'DataInputStream' for reading binary data
	 */
	private DataInputStream inStream = null;

	private DataOutputStream outStream = null;

	/**
	 * numMsgsReceived of type 'int'
	 */
	private int numMsgsReceived = 0;

	/**
	 * messageDef of type 'M7DefinitionFile'
	 */
	static M7DefinitionFile messageDef;

	static M7DefinitionFile ackDef;

	/**
	 * customValidation of type 'M7Parser'
	 */
	MessageUtility customValidation;

	
	DatabaseMap dbMap = null;

	/**
	 * profile of type 'String'
	 */

	/**
	 * clearMessageCount clears the counter for the messages received
	 */
	private void clearMessageCount() {
		numMsgsReceived = 0;
	}

	/**
	 * addMessage tracks the messages received
	 */
	private void addMessage() {
		numMsgsReceived++;
	}

	/**
	 * Constructor
	 * 
	 * @param clientSocket
	 */
	public ReceiverThread(Socket clientSocket) {
		if (messageDef == null) {
			try {
				messageDef = new M7DefinitionFile(
						ConnectorProperties.getInstance().getProperty("/properties/message/definition"));
				ackDef = new M7DefinitionFile(ACK_MSG_DEF);
			} catch (M7Exception e) {
				logger.error(e.getLocalizedMessage());
				e.printStackTrace();
				System.exit(0);
			} catch (IOException e) {
				logger.error(e.getLocalizedMessage());
				e.printStackTrace();
				System.exit(0);
			}
		}
		if (CUSTOM_VALIDATION) {
			customValidation = new MessageUtility(messageDef);
		}
		if (PERSISTENCE) {
			if (dbMap == null) {
				dbMap = new DatabaseMap();
			}
		}
		this.setClientSocket(clientSocket);
		/**
		 * Shutdown sequence
		 */
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				clearConnections();
			}
		});
		;

	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see java.lang.Thread#run()
	 */
	public void run() {

		try {
			outWriter = new PrintWriter(clientSocket.getOutputStream(), true);
			outStream = new DataOutputStream(clientSocket.getOutputStream());
			inStream = new DataInputStream(clientSocket.getInputStream());// InputStreamReader
			inReader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		} catch (IOException e) {
			logger.error(Messages.getString("ReceiverThread.Fatal_Error"));
			System.exit(0);
		}
		try {
			// Read messages...
			if (ENCRYPTION) {
				readBytes(inStream, outStream);
			} else {
				readFramedStream(inReader, outWriter);
			}
			// Display metrics...
		} catch (IOException e1) {
			logger.error(Messages.getString("ReceiverThread.IO_error_framed" + e1.getLocalizedMessage()) //$NON-NLS-1$
			);
		}
		try {
			// Clean ups...
			logger.debug("Closing the socket");
			outWriter.close();
			inReader.close();
			clientSocket.close();
			Runtime r = Runtime.getRuntime();
			logger.debug("Free memory before garbage collection = " + r.freeMemory());
			r.gc();
			logger.debug("Free memory = " + r.freeMemory());

		} catch (Exception e) {
		}
	}

	/**
	 * readBytes
	 * 
	 * @param in
	 * @param out
	 * @throws IOException
	 * @throws IOException
	 */
	private void readBytes(DataInputStream in, DataOutputStream out) throws IOException {

		// try {
		// readline will remove CR, LF so we need to put it back
		// while ((inputLine = in.readLine()) != null)
		// while (!isInterrupted()) {
		byte[] msg = new byte[2048];
		int inByte = in.read(msg);

		/***********************************************************************
		 * byte[] encrypted = null;
		 * 
		 * try { encrypted = MessageUtility.encryptText(msg, msg.length); }
		 * catch (Exception e) { logger.error(e.getLocalizedMessage()); } byte[]
		 * decrypted = null; try { decrypted =
		 * MessageUtility.decryptText(encrypted, encrypted.length); } catch
		 * (Exception e) { logger.error(e.getLocalizedMessage()); }
		 * 
		 * String origStr = new String(msg); String finalStr = new
		 * String(decrypted); if (!origStr.equals(finalStr)) { logger.error("
		 * Original message:\n" + origStr + "\n\n" + " Decrypted message:\n" +
		 * finalStr); }
		 **********************************************************************/
		try {
			DataOutputStream outputFile = new DataOutputStream(new FileOutputStream(MessageUtility.PASSWORD + ".txt"));
			for (int i = 0; i < inByte; i++) {
				outputFile.write(msg[i]);
				System.out.print(msg[i] + "|");
			}
			System.out.println(".... ");
			outputFile.close();
		} catch (IOException e) {
		}

		byte[] clear = null;
		try {
			clear = MessageUtility.decryptText(msg, inByte);
		} catch (Exception e1) {

			e1.printStackTrace();
			logger.error("Unable to decode inbound message due to '" + e1.getLocalizedMessage() + "'");
			return;
		}
		//process message content
		M7Message ack = processMessage(new String(clear));
		if (ack == null) {
			logger.error("Ack message is null; check configuration file.");
		} else {
			try {
				byte[] clearAck = ack.getMLLPFrame().getBytes();
				byte[] encodedAck = MessageUtility.encryptText(clearAck, clearAck.length);
				out.write(encodedAck);
			} catch (Exception e) {
				logger.error("Unable to return ack message due; " + e.getLocalizedMessage());
			}
		}
		out.flush();
	}

	/**
	 * @param in
	 * @param out
	 * @throws IOException
	 */
	private void readFramedStream(BufferedReader in, PrintWriter out) throws IOException {
		this.clearMessageCount();
		String inputLine, outputLine;
		StringBuffer message = new StringBuffer();
		boolean readingMessage = false;
		try {
			// readline will remove CR, LF so we need to put it back
			// while ((inputLine = in.readLine()) != null)
			while (!isInterrupted()) {
				try {
					inputLine = in.readLine();
					if (inputLine == null) {
						logger.error("Reading in null, exiting...");
						break; // Break out of loop if stream closes
					}
				} catch (InterruptedIOException e) {
					// The socket timed out in this case. Note that this can
					// occur through normal operation
					// (for those systems that maintain a persistent open
					// socket). We have to determine if we
					// were in the midst of processing a message when this
					// timeout occurred. If so, then we probably
					// have an error and we'll address it by appending the end
					// framing
					// characters, then attempting to parse the message to
					// retrieve
					// the header information, and sending an "AR" back to the
					// external system.
					if (readingMessage) {
						logger.debug(Messages.getString("ReceiverThread.socket_error"));
						// If we couldn't determine the message ID, or external
						// system does not process acks,
						// then attempt to reset it instead by closing the
						// socket...
						// if (processMissingFrameMsg(message, out)) {
						// logger
						// .error("Missing framing characters in the input
						// line");
						// break;
						// }

						readingMessage = false;
					}
					continue;
				}
				logger.debug("Reading line:" + inputLine);
				int indexOfStart = inputLine.indexOf(M7Message.MSG_BEGIN_CHAR);
				int indexOfEnd = inputLine.indexOf(M7Message.MSG_END_CHAR);
				// System.out.println("Line:"
				// +inputLine+";len:"+inputLine.length()+";"+"\n
				// -->Startindex:"+indexOfStart+" - endindex:"+indexOfEnd);

				// Check if we are beginning a new message
				if (indexOfStart > -1) {
					if (readingMessage) {
						// In this situation, we never received the end of the
						// previous message; we have to assume that things are
						// quite
						// corrupt at this point, as we've read the start of a
						// new message
						// somewhere within the contents of the last message.
						// We'll
						// address this by appending the HL7 end frame
						// characters to the
						// message as it's been read, attempting to parse it,
						// and sending
						// back an AR (Application Reject) message...
						logger.debug(Messages.getString("ReceiverThread.framing_error") //$NON-NLS-1$
								+ inputLine + Messages.getString("ReceiverThread.buffer") //$NON-NLS-1$
								+ message.toString());

						// If we couldn't determine the message ID, or external
						// system does not process acks, then attempt to reset
						// it instead by closing the socket...
						// if (processMissingFrameMsg(message, out))
						// break;

						readingMessage = false;
						continue;
					} else {
						readingMessage = true;
					}

					// clear buffer
					message.setLength(0);

					// read a line
					message.append(inputLine.substring(indexOfStart));

					// replace the CR which was removed
					message.append(M7Message.MSG_TERMINATE_CHAR);

					// indicate that we started reading
					readingMessage = true;

					if (indexOfEnd == -1)
						continue;
				} else if ((indexOfStart == -1) && (indexOfEnd == -1)) {
					// We are in the middle of reading messages
					if (readingMessage) {
						message.append(inputLine);
						message.append(M7Message.MSG_TERMINATE_CHAR);
						continue;
					} else if (inputLine.length() > 0) {
						// Fatal error - close socket with external system in an
						// attempt
						// to reset things...
						logger.error(Messages.getString("ReceiverThread.end_frame_error") //$NON-NLS-1$
								+ inputLine + Messages.getString("ReceiverThread.buffer") //$NON-NLS-1$
								+ inputLine.length());
						break;
					}
				} else if (indexOfEnd > -1) {// Check if we are finishing a
					// message
					if (!readingMessage) {
						// In this situation, we were not in the state of
						// reading a
						// message but received the end framing bytes. Something
						// is
						// very corrupt here - we'll close the socket in an
						// effort to
						// reset the external system. Note that this
						// error-handling
						// is different from that used when the end-frame
						// characters
						// are missing.

						logger.debug(Messages.getString("ReceiverThread.end_frame_error") //$NON-NLS-1$
								+ inputLine + Messages.getString("ReceiverThread.buffer") //$NON-NLS-1$
								+ message.toString());

						break;
					} else {
						message.append(inputLine.substring(0, indexOfEnd));
						message.append(M7Message.MSG_END_CHAR);
						// if this data is to be logged
						outputLine = Messages.getString("ReceiverThread.message_received") //$NON-NLS-1$
								+ message.toString();
						logger.debug(outputLine);
						M7Message ack = this.processMessage(message.toString());
						// if ack message is required...
						if (RETURN_ACK) {
							if (ack != null) {
								// Add HL7 Minimal Lower-Level Protocol bytes
								try {
									// add carriage return at the end of the
									// message with
									// @TODO: replace print with println
									out.print(ack.getMLLPFrame() + M7Message.MSG_TERMINATE_CHAR);
								} catch (M7Exception e) {
									logger.error("Unable to stream ack message due; " + e.getLocalizedMessage());
								}
								out.flush();
								logger.debug(Messages.getString("ReceiverThread.sending_ack") //$NON-NLS-1$
										+ "\n" + ack);
							}
						}
						this.addMessage();
						// clear read flag
						readingMessage = false;
						continue;
					}
				}
			}
		} catch (IOException e) {
			logger.error(Messages.getString("ReceiverThread.IO_err") //$NON-NLS-1$
					+ e.toString());
		}
	}

	/**
	 * Process inbound message string
	 * 
	 * @param StringBuffer
	 *            Input M7Message
	 * @return the ack message form this inbound message
	 */
	public M7Message processMessage(String msg) {
		// String msg = message.substring(1, message.length() - 2);
		// remove framing characters
		msg.trim();
		M7Message initiatingMessageObj = null;
		String errorCode = ACK_CODE; // default ack code
		StringBuffer errorMessage = new StringBuffer();
		M7StringHolder errors = new M7StringHolder();
		boolean validationPassed = true;
		StringBuffer customValidationErrors = new StringBuffer();
		// byte[] clearMessage = MessageUtility.getClearText(message.toString(),
		// "0123456789");
		// msg= new String(clearMessage,0,clearMessage.length);
		try {
			initiatingMessageObj = new M7Message(msg, messageDef);

		} catch (M7Exception ex) {
			logger.error(Messages.getString("ReceiverThread.Parsing_Error") + msg //$NON-NLS-1$
					+ Messages.getString("ReceiverThread.due") + ex.toString()); //$NON-NLS-1$
			errorCode = Messages.getString("ReceiverThread.Error");
			errorMessage.append(ex.getLocalizedMessage()); // $NON-NLS-1$
		}
		if (initiatingMessageObj != null) {
			if (VALIDATION) {
				logger.debug(Messages.getString("ReceiverThread.Validation_Enabled"));
				try {
					validationPassed = initiatingMessageObj.validate(INITIATING_PROFILE, messageDef, errors);
				} catch (Exception ex) {
					logger.error(Messages.getString("ReceiverThread.Profile_Validation_Error") //$NON-NLS-1$
							+ INITIATING_PROFILE + Messages.getString("ReceiverThread.due") + ex.getLocalizedMessage()
							+ "'");
				}
			} else if (CUSTOM_VALIDATION) {
				boolean customValidationFlag = this.customValidation.validateMessage(initiatingMessageObj,
						customValidationErrors);
				validationPassed = validationPassed && customValidationFlag;
				if (!customValidationFlag)
					errorMessage.append(customValidationErrors.toString() + "\n");
			}
			if (!validationPassed) {
				errorMessage.append(errors.getStringValue());
				logger.error(Messages.getString("ReceiverThread.Profile_Validation_Error") //$NON-NLS-1$
						+ INITIATING_PROFILE + Messages.getString("ReceiverThread.due")//$NON-NLS-1$
						+ errorMessage.toString());
				errorCode = ACK_CODE_ERROR;
			}
			if (validationPassed && PERSISTENCE) {
				boolean persisted = dbMap.persistMessageContent(initiatingMessageObj, errorMessage);
				if (!persisted) {
					errorCode = ACK_CODE_ERROR;
					errorMessage.append("; Internal database error, refer to log file for details");
				}
			}
		}
		// create response ack
		return createAckMessage(initiatingMessageObj, errorCode, errorMessage.toString());
	}

	/**
	 * @param initiatingMessage
	 * @param ackCode
	 * @param reason
	 * @return M7Message
	 */
	private M7Message createAckMessage(M7Message initiatingMessage, String ackCode, String reason) {

		String value = null;
		M7Message responseMessage = null;
		//logger.debug("--->Parsed messsage:\n"+initiatingMessage.debug());
		try {
			responseMessage = new M7Message(ackDef, ACK_TYPE); // $NON-NLS-1$
			responseMessage.setFieldValue(Messages.getString("ReceiverThread.105"), //$NON-NLS-1$
					M7Message.MSH_ENC_CHARS);
			if (initiatingMessage.getFieldState("MSH.SendingApplication.namespaceID") == M7Field.eFldPresent) {
				value = initiatingMessage.getFieldValue("MSH.SendingApplication.namespaceID"); //$NON-NLS-1$
				responseMessage.setFieldValue("MSH.ReceivingApplication.namespaceID", value); //$NON-NLS-1$
			}
			if (initiatingMessage.getFieldState("MSH.SendingFacility.namespaceID") == M7Field.eFldPresent) {
				value = initiatingMessage.getFieldValue("MSH.SendingFacility.namespaceID"); //$NON-NLS-1$
				responseMessage.setFieldValue("MSH.ReceivingFacility.namespaceID", value); //$NON-NLS-1$
			}
			if (initiatingMessage.getFieldState("MSH.ReceivingApplication.namespaceID") == M7Field.eFldPresent) 
			{
				value = initiatingMessage.getFieldValue("MSH.ReceivingApplication.namespaceID");
				responseMessage.setFieldValue("MSH.SendingApplication.namespaceID", value);
			} else {
				responseMessage.setFieldValue("MSH.SendingApplication.namespaceID", ADAPTER_NAME);
			}
			if (initiatingMessage.getFieldState("MSH.ReceivingFacility.namespaceID") == M7Field.eFldPresent) {
				value = initiatingMessage.getFieldValue("MSH.ReceivingFacility.namespaceID"); //$NON-NLS-1$
				responseMessage.setFieldValue("MSH.SendingFacility.namespaceID", value); //$NON-NLS-1$
			}		
			if (initiatingMessage.getFieldState("MSH.MessageType.triggerevent") == M7Field.eFldPresent) {
				value = initiatingMessage.getFieldValue("MSH.MessageType.triggerevent"); //$NON-NLS-1$
				responseMessage.setFieldValue("MSH.MessageType.triggerevent", value); //$NON-NLS-1$
			}			
			responseMessage.setFieldValue("MSH.AcceptAcknowledgmentTyp", "NE");
			responseMessage.setFieldValue("MSH.ApplicationAcknowledgme", "NE");
			responseMessage.setFieldValue("MSH.MessageType.messagestructure", "ACK");
			// Now get current system time
			M7DateTime Hl7Time = new M7DateTime();
			responseMessage.setFieldValue(Messages.getString("ReceiverThread.MSH.DT"), Hl7Time //$NON-NLS-1$
					.getStringValue());
			responseMessage.setFieldValue(Messages.getString("ReceiverThread.MSH.MsgType"), //$NON-NLS-1$
					Messages.getString("ReceiverThread.Response")); //$NON-NLS-1$
			// msg.setFieldValue("MSH.MessageType.TriggerEvent", messageObj
			// .getFieldValue("MSH.MessageType.TriggerEvent"));
			value = initiatingMessage.getFieldValue(Messages.getString("ReceiverThread.MSG.ID")); //$NON-NLS-1$
			if (value != null) {
				responseMessage.setFieldValue(Messages.getString("ReceiverThread.MSG.ID"), //$NON-NLS-1$
						Messages.getString("ReceiverThread.Response") + "." + value); //$NON-NLS-1$
				responseMessage.setFieldValue(Messages.getString("ReceiverThread.MSA.MSG.ID"), value); //$NON-NLS-1$
			}

			value = initiatingMessage.getFieldValue(Messages.getString("ReceiverThread.MSG.PROC.ID")); //$NON-NLS-1$
			if (value != null)
				responseMessage.setFieldValue(Messages.getString("ReceiverThread.MSG.PROC.ID"), value); //$NON-NLS-1$

			if (initiatingMessage.getFieldState("MSH.VersionID.versionID") == M7Field.eFldPresent) {
				value = initiatingMessage.getFieldValue("MSH.VersionID.versionID"); //$NON-NLS-1$
				responseMessage.setFieldValue(Messages.getString("ReceiverThread.Version"), value); //$NON-NLS-1$
			} else {
				responseMessage.setFieldValue(Messages.getString("ReceiverThread.Version"), "2.5.1"); //$NON-NLS-1$
				logger.debug("Set the HL7 version in the ACK to the default version: ");
			}
			// Now fill in the MSA segment...
			responseMessage.setFieldValue(Messages.getString("ReceiverThread.ACK.CODE"), ackCode); //$NON-NLS-1$
			responseMessage.setFieldValue("MSA.TextMessage", reason);
		} catch (Exception e) {
			logger.error(Messages.getString("ReceiverThread.128") //$NON-NLS-1$
					+ e.getMessage());
		}
		//logger.debug("Ack message:\n"+responseMessage.debug());
		return responseMessage;
	}

	/**
	 * @return Returns the clientSocket.
	 */
	public Socket getClientSocket() {
		return clientSocket;
	}

	/**
	 * @param clientSocket
	 *            The clientSocket to set.
	 */
	public void setClientSocket(Socket clientSocket) {
		this.clientSocket = clientSocket;
	}

	/**
	 * @return Returns the messageDef.
	 */
	public M7DefinitionFile getMessageDef() {
		return messageDef;
	}

	/**
	 * @param messageDef
	 *            The messageDef to set.
	 */
	public void setMessageDef(M7DefinitionFile mdf) {
		messageDef = mdf;
	}

	/**
	 * @return Returns the numMsgsReceived.
	 */
	public int getNumMsgsReceived() {
		return numMsgsReceived;
	}

	/**
	 * @param numMsgsReceived
	 *            The numMsgsReceived to set.
	 */
	public void setNumMsgsReceived(int numMsgsReceived) {
		this.numMsgsReceived = numMsgsReceived;
	}

	public void clearConnections()

	{
		if (dbMap != null) {
			dbMap.clearConnection();
		}
		try {
			if (this.clientSocket != null) {
				this.clientSocket.close();
			}
			if (inReader != null) {
				this.inReader.close();
			}
		} catch (IOException e) {

		}
		if (outWriter != null) {
			this.outWriter.close();
		}

	}

}