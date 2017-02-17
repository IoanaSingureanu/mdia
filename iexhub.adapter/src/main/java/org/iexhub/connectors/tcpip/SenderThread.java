
package org.iexhub.connectors.tcpip;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
//Java 7+ imports
//import java.nio.charset.Charset;
//import java.nio.charset.StandardCharsets;
//import java.nio.file.Files;
//import java.nio.file.Paths;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.iexhub.connectors.database.DatabasePoller;
import org.iexhub.connectors.tcpip.common.ConnectorProperties;
import org.iexhub.connectors.tcpip.common.MessageUtility;

import com.eversolve.Medi7.M7DefinitionFile;
import com.eversolve.Medi7.M7Exception;
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
public class SenderThread extends Thread {

	static Logger logger = Logger.getLogger(SenderThread.class);

	public static String TEST_FILE = null;

	public static String HOST = "127.0.0.1";

	public static int PORT = 4000;

	public static boolean ACK_ENABLED = true;

	public static long DELAY = 10000;

	public static long RETRY = 1000;

	public static int REPETITIONS = 200;

	public static int TIMEOUT = 2;

	public static boolean DISCONNECT_ON_TIMEOUT = true;
	
	public static String MSG_DEF_FILE_NAME  = null;

	Socket clientSocket = null;

	PrintWriter out = null;

	BufferedReader in = null;

	static DatabasePoller dbPoller = null;

	static {
		TEST_FILE = ConnectorProperties.getInstance().getProperty("/properties/host/test/file");
		HOST = ConnectorProperties.getInstance().getProperty("/properties/host/name");
		PORT = Integer.parseInt(ConnectorProperties.getInstance().getProperty("/properties/host/port"));
		RETRY = Long.parseLong(ConnectorProperties.getInstance().getProperty("/properties/host/retry"));
		TIMEOUT = Integer.parseInt(ConnectorProperties.getInstance().getProperty("/properties/host/timeout"));
		ACK_ENABLED = ConnectorProperties.getInstance().getProperty("/properties/ack/mode").equalsIgnoreCase("AL");
		DELAY = Long.parseLong(ConnectorProperties.getInstance().getProperty("/properties/message/database/polling/interval"));
		DISCONNECT_ON_TIMEOUT = Boolean
				.parseBoolean(ConnectorProperties.getInstance().getProperty("/properties/host/disconnectOnTimeout"));
		MSG_DEF_FILE_NAME = ConnectorProperties.getInstance().getProperty("/properties/message/definition");
	}

	/**
	 * Sender Thread
	 */
	public SenderThread() {
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
	
		connectSocket();
		if (TEST_FILE != null) {
			int messageCount=0;
			while(messageCount<REPETITIONS)
			{
			M7Message msg = null;
 			try {
				//String content = readFile(TEST_FILE, StandardCharsets.UTF_8); //java 7
 				String content = MessageUtility.readFile(TEST_FILE,"UTF-8");
				M7DefinitionFile mdf = new M7DefinitionFile(MSG_DEF_FILE_NAME);
				msg = new M7Message(content, mdf);
			} catch (Exception e) {
				e.printStackTrace();
				logger.error(e.getLocalizedMessage());
			}
			if (msg != null)
				{
				sendTestMessage(msg);				
				}
			messageCount++;
			try {
				sleep(DELAY);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			}
		} 
		else // poll the database for outstanding messages
		{
			if (dbPoller == null) {
				dbPoller = new DatabasePoller(DatabasePoller.MSG_DEF_FILE_NAME, DatabasePoller.ACK_DEF_FILE_NAME);
				logger.info("Thread (" + this.getId() + ") connecting to '" + HOST + ":" + PORT + "'");
			}
			Vector<M7Message> inputMessageVector = null;
			try {
				while (true) {
					// poll for new messages from the database
					inputMessageVector = dbPoller.poll();
					if (inputMessageVector == null) {
						logger.debug("Unable to connect to the db");
						dbPoller.clearConnection();
						dbPoller.connect();
					} else {
						// Metrics info
						int numMsgsSent = 0;
						for (int j = 0; j < inputMessageVector.size(); j++) {
							M7Message intiatingMessage = (M7Message) inputMessageVector.elementAt(j);
							// Measure each round-trip elapsed time if we're processing acks
							boolean messageSent = false;
							try {
								String outputMessageFramed = intiatingMessage.getMLLPFrame();
								out.println(outputMessageFramed);
								messageSent = true;
								numMsgsSent++;
								logger.debug("Sent message(" + numMsgsSent + "):\n" + outputMessageFramed);
								logger.debug(intiatingMessage.debug());
							} catch (M7Exception e) {
								logger.error(
										"Unable to parse the initiating message due to " + e.getLocalizedMessage());

							}
							if (messageSent && ACK_ENABLED) {
								String returnedAckLine;
								StringBuffer message = new StringBuffer();
								boolean readingMessage = false;
								logger.debug("Waiting for response message...");
								// reset the counter
								int counter = 0;
								while ((!in.ready()) && (counter != TIMEOUT)) {
									sleep(1000);
									counter++;
								}
								if (counter == TIMEOUT) {
									logger.info("M7Message transmission timed out after " + counter + " seconds");
									try {
										dbPoller.updateRow(intiatingMessage.getFieldValue(dbPoller.getMessageIdField()),
												dbPoller.getResendCode(), "Timeout");
									} catch (M7Exception e) {
										e.printStackTrace();
										logger.error("Unable to read the message control id");
										System.exit(0);
									}
									if (DISCONNECT_ON_TIMEOUT) {
										logger.debug("Reconnecting...");
										this.reconnectSocket();
									}
								} else {
									while ((returnedAckLine = in.readLine()) != null) {
										int indexOfStart = returnedAckLine.indexOf(11, 0);
										int indexOfEnd = returnedAckLine.indexOf(28, 0);
										// Check if we are beginning a new message
										if (indexOfStart > -1) {
											if (readingMessage) {
												// it appears that we never received the end of the previous message
												logger.error(
														"Received a new message without finishing the previous - incomplete message."
																+ "Input line:\n" + returnedAckLine + "\n\t"
																+ "M7Message Buffer content:" + message.toString());
											} else {
												readingMessage = true;
											}
											// clear buffer
											message.setLength(0);
											// read a line
											message.append(returnedAckLine.substring(indexOfStart));
											// replace the CR which was removed
											message.append(M7Message.MSG_TERMINATE_CHAR);
											// indicate that we started reading
											readingMessage = true;
										}
										// Check if we are finishing a message
										else if (indexOfEnd > -1) {
											if (!readingMessage) {
												// we were not reading a message
												// but
												// still
												// received the end
												logger.warn("Incomplete message - ended a message without starting:\n"
														+ returnedAckLine + "\n\t" + ". M7Message Buffer content:"
														+ message.toString());
											} else {
												message.append(returnedAckLine.substring(0, indexOfEnd));
												message.append(M7Message.MSG_END_CHAR);
												// message completed
												logger.debug(
														"Received complete response message:\n" + message.toString());

												dbPoller.updateResponse(message.toString());
												// clear read flag
												// readingMessage = false;
												break;
											}
										} else // we are in the middle of
												// reading
										// messages
										{
											if (readingMessage) {
												message.append(returnedAckLine);
												message.append(M7Message.MSG_TERMINATE_CHAR);
											} else if (returnedAckLine.length() > 0)

												logger.error("Received orphan line:\n" + returnedAckLine + "--size:"
														+ returnedAckLine.length());
										}
									}
								}
							} else {
								// no ack required and message not sent
							}
						} // message iteration
					} // if the vector is not null
					Runtime r = Runtime.getRuntime();
					logger.debug("Free memory before garbage collection = " + r.freeMemory());
					r.gc();
					logger.debug("Free memory = " + r.freeMemory());
					// Now wait for a while
					sleep(DELAY);

				} // while(true)
			} catch (IOException e1) {

				e1.printStackTrace();
				logger.error(e1.getLocalizedMessage());
			} catch (InterruptedException e1) {

				e1.printStackTrace();
				logger.error(e1.getLocalizedMessage());
			}
			// Clean up 
			try {
				if (out != null) {
					out.close();
				}
				if (in != null) {
					in.close();
				}
				if (clientSocket != null) {
					clientSocket.close();
				}
				dbPoller.clearConnection();
			} catch (IOException e) {

				e.printStackTrace();
				logger.error(e.getLocalizedMessage());
			}

		} // completed database poll
	}

	/**
	 * Drop the connection and reconnect
	 */
	protected void reconnectSocket() {
		try {
			this.in.close();
		} catch (IOException e) {
			logger.debug("Error while closing the input buffer; " + e.getLocalizedMessage());
		}
		try {
			this.clientSocket.close();
		} catch (IOException e) {
			logger.debug("Error while disconnecting socket; " + e.getLocalizedMessage());
		}

		this.out.close();
		this.clientSocket = null;
		this.in = null;
		this.out = null;
		this.connectSocket();
	}

	/**
	 * Connect the socket and initiate the input and output buffered reader
	 */
	protected void connectSocket() {
		while (clientSocket == null) {
			try {
				clientSocket = new Socket(HOST, PORT);
			} catch (UnknownHostException e1) {
				logger.error("Configuration error: invalid host '" + HOST + "' " + e1.getLocalizedMessage());
				System.exit(0);
			} catch (IOException e1) {
				logger.error("Unable to connect to " + HOST + ":" + PORT + " - " + e1.getLocalizedMessage());
			}
			if (clientSocket == null) {
				logger.debug("Retrying the connection in " + RETRY / 1000 + " seconds.");

				try {
					sleep(RETRY);
				} catch (InterruptedException e) {
				}
			} //
		}
		try {
			out = new PrintWriter(clientSocket.getOutputStream(), true);
		} catch (IOException e2) {
			logger.error(e2);
			e2.printStackTrace();
		}
		try {
			in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
		} catch (IOException e3) {
			logger.error(e3);
			e3.printStackTrace();
		}
	}

	/**
	 * clearConnections
	 */
	public void clearConnections() {

		if (dbPoller != null) {
			dbPoller.clearConnection();
		}
		if (this.in != null) {
			try {
				this.in.close();
			} catch (IOException e) {

			}
		}
		if (this.out != null) {
			this.out.close();
		}
		if (this.clientSocket != null) {
			try {
				this.clientSocket.close();
			} catch (IOException e) {

			}
		}
	}

	
	void sendTestMessage(M7Message intiatingMessage) {
		try {
			boolean messageSent = false;
			try {
				String outputMessageFramed = intiatingMessage.getMLLPFrame();
				out.println(outputMessageFramed);
				messageSent = true;
				logger.debug(intiatingMessage.debug());
			} catch (M7Exception e) {
				logger.error("Unable to parse the initiating message due to " + e.getLocalizedMessage());

			}

			if (messageSent && ACK_ENABLED) {
				String returnedAckLine;
				StringBuffer message = new StringBuffer();
				boolean readingMessage = false;
				logger.debug("Waiting for response message...");
				// reset the counter
				int counter = 0;
				while ((!in.ready()) && (counter != TIMEOUT)) {
					sleep(1000);
					counter++;
				}
				if (counter == TIMEOUT) {
					logger.info("M7Message transmission timed out after " + counter + " seconds");
					
					if (DISCONNECT_ON_TIMEOUT) {
						logger.debug("Reconnecting...");
						this.reconnectSocket();
					}
				} else {
					while ((returnedAckLine = in.readLine()) != null) {
						int indexOfStart = returnedAckLine.indexOf(11, 0);
						int indexOfEnd = returnedAckLine.indexOf(28, 0);

						// Check if we are beginning a new
						// message
						if (indexOfStart > -1) {
							if (readingMessage) {
								// it appears that we never
								// received
								// the
								// end
								// of the previous message
								logger.error(
										"Received a new message without finishing the previous - incomplete message."
												+ "Input line:\n" + returnedAckLine + "\n\t"
												+ "M7Message Buffer content:" + message.toString());
							} else {
								readingMessage = true;
							}

							// clear buffer
							message.setLength(0);
							// read a line
							message.append(returnedAckLine.substring(indexOfStart));
							// replace the CR which was removed
							message.append(M7Message.MSG_TERMINATE_CHAR);
							// indicate that we started reading
							readingMessage = true;
						}
						// Check if we are finishing a message
						else if (indexOfEnd > -1) {
							if (!readingMessage) {
								// we were not reading a message
								// but
								// still
								// received the end
								logger.warn("Incomplete message - ended a message without starting:\n" + returnedAckLine
										+ "\n\t" + ". M7Message Buffer content:" + message.toString());
							} else {
								message.append(returnedAckLine.substring(0, indexOfEnd));
								message.append(M7Message.MSG_END_CHAR);
								// message completed
								logger.debug("Received complete response message:\n" + message.toString());

								break;
							}
						} else // we are in the middle of
								// reading
						// messages
						{
							if (readingMessage) {
								message.append(returnedAckLine);
								message.append(M7Message.MSG_TERMINATE_CHAR);
							} else if (returnedAckLine.length() > 0)

								logger.error("Received orphan line:\n" + returnedAckLine + "--size:"
										+ returnedAckLine.length());
						}
					}
				}
			} else {
				// no ack required and message not sent
			}
		} catch (IOException e1) {

			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		} catch (InterruptedException e1) {

			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		}

	}
}
