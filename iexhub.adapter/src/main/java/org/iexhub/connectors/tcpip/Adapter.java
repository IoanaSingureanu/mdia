
package org.iexhub.connectors.tcpip;

import java.util.Vector;

import org.apache.log4j.Logger;
import org.iexhub.connectors.tcpip.common.ConnectorProperties;

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
public class Adapter {

	static Logger logger = Logger.getLogger(Adapter.class);

	public static final int NUM_THREADS = 1;

	static String CLIENT_TYPE_RECEIVER = "RECEIVER";

	static String CLIENT_TYPE_SENDER = "SENDER";

	static String clientType = CLIENT_TYPE_RECEIVER;
	
	static Vector<SenderThread> threadPool = new Vector<SenderThread>();
	
	static ListenerThread listener = null;
	static {
		ConnectorProperties.initProperties(System.getProperty("PROPERTIES"));

	}

	/**
	 * The main entry point for the application.
	 * 
	 * @param args
	 *            Array of parameters passed to the application via the command
	 *            line. Default behavior: Sender/TCP/IP client
	 */
	public static void main(String[] args) {

		/**
		 * User can specify "sender" or "listener" mode either from the command
		 * line or the properties file. Note that the command-line specification
		 * overrides the properties file entry.
		 */
		if (args.length > 1) {
			System.err
					.println("Only one command-line parameter supported - \"SENDER\" for sender mode, \"RECEIVER\" for receiver mode");
			System.exit(0);
		} else if (args.length == 1) {

			setClientType(args[0]);

		} else if (args.length == 0) {
			// Default = LISTENER

		}

		logger.info("Adapter running in " + getClientType() + " mode.");
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				clearThreads();
			}
		});
		;
		/**
		 * We have the following configurable settings: // - IP address/hostname // -
		 * Port number // - input data file // - number of broadcast threads // -
		 * time delay (in milliseconds) between record transmissions // -
		 * repetition count // - acknowledgement message processing
		 */
		// Now determine if this should be a sender or receiver
		if (getClientType().equals(CLIENT_TYPE_SENDER)) {
			

			// Now spawn x number of threads (from properties)
			for (int i = 0; i < NUM_THREADS; i++) {

				SenderThread thread = new SenderThread();
				threadPool.addElement(thread);
				thread.start();

			}
			// Now wait for the threads to complete...
			try {
				for (int i = 0; i < NUM_THREADS; i++)
					((SenderThread) threadPool.elementAt(i)).join();
			} catch (IllegalMonitorStateException e) {
				logger.error("Serious threading problem:"
						+ e.getLocalizedMessage());
				System.exit(0);
			} catch (InterruptedException e) {
				logger.error("Serious threading problem:"
						+ e.getLocalizedMessage());
				System.exit(0);
			}
		} else {
			// listener port
			int listenerPort = Integer.parseInt(ConnectorProperties.getInstance()
					.getProperty("/properties/socket/port"));
			listener = new ListenerThread(listenerPort);
			listener.start();

			// Now wait for the threads to complete...
			try {
				listener.join();
			} catch (IllegalMonitorStateException e) {
				logger.error("Serious threading problem:"
						+ e.getLocalizedMessage());
				System.exit(0);
			} catch (InterruptedException e) {
				logger.error("Serious threading problem:"
						+ e.getLocalizedMessage());
				System.exit(0);
			}
		}
	}

	/**
	 * @return Returns the clientType.
	 */
	public static String getClientType() {
		return clientType;
	}

	/**
	 * Set client as receiver or sender
	 * 
	 * @param type
	 *            The clientType to set.
	 */
	public static void setClientType(String type)
			throws IllegalArgumentException {
		if ((clientType.equals(CLIENT_TYPE_RECEIVER))
				|| (clientType.equals(CLIENT_TYPE_SENDER))) {
			clientType = type;
		}

		else {
			new IllegalArgumentException(
					"Only one command-line parameter supported - \"SENDER\" for sender mode, \"RECEIVER\" for receiver mode");
			System.exit(0);
		}

	}
	/**
	 * Invoked when the adapter shuts down
	 */
	public static void clearThreads()
	{
		if(threadPool!=null)
		{
			
		}
		if(listener !=null)
		{
			//listener.join();
		}
	}
}
