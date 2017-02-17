
package org.iexhub.connectors.tcpip;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;

import org.apache.log4j.Logger;
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
public class ListenerThread extends Thread {
	static Logger logger = Logger.getLogger(ListenerThread.class);

	/**
	 * Default listenerPort of type 'int' is set to 4000
	 */
	int listenerPort = 4000;

	/**
	 * Unlimited timeout
	 */
	public static final int LISTENER_TIMEOUT = 0;

	/**
	 *  serverSocket of type 'ServerSocket'
	 */
	private ServerSocket serverSocket = null;


	/**
	 * @param port
	 */
	public ListenerThread(int port) {

		this.setListenerPort(port);
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				clearConnections();
			}
		});
		;
	}

	/* (non-Javadoc)
	 * @see java.lang.Thread#run()
	 */
	public void run() {

		// Create server socket...
		try {
			serverSocket = new ServerSocket(this.getListenerPort());
		} catch (IOException e) {
			logger.error(Messages.getString("ListenerThread.6") //$NON-NLS-1$
					+ this.getListenerPort()); //$NON-NLS-1$
			System.exit(0);
		}
		Socket clientSocket = null;

		while (!isInterrupted()) {
			try {

				logger
						.info(Messages.getString("ListenerThread.8") + this.getListenerPort() + Messages.getString("ListenerThread.9")); //$NON-NLS-1$
				clientSocket = serverSocket.accept();
				// Enable TCP SO_KEEPALIVE...
				clientSocket.setKeepAlive(true);
				// Set timeout value for socket reads (value of "0" denotes
				// infinite wait)
				clientSocket.setSoTimeout(LISTENER_TIMEOUT);
				// MessageMonitor statistics
				// monitorListener.incrementNumSystemsConnectedCount();
				logger.info(Messages.getString("ListenerThread.Client_Connected")); //$NON-NLS-1$
			} catch (IOException e) {
				logger.error(Messages.getString("ListenerThread.socket_accept_error")); //$NON-NLS-1$
				System.exit(0);
			}
		
			Runtime r = Runtime.getRuntime();
			logger.debug("Free memory before garbage collection = "
					+ r.freeMemory());
			r.gc();
			logger.debug("Free memory = " + r.freeMemory());
   
           // Now spawn a new thread and pass it the socket...
			ReceiverThread thread = new ReceiverThread(clientSocket);
			thread.start();
			
		}

		try {
			serverSocket.close();
			
		} catch (IOException e) {
			System.err.println(Messages.getString("ListenerThread.unable_to_close_socket")); //$NON-NLS-1$
		}
	}

	/**
	 * @return Returns the serverSocket.
	 */
	public ServerSocket getServerSocket() {
		return serverSocket;
	}

	/**
	 * @param serverSocket
	 *            The serverSocket to set.
	 */
	public void setServerSocket(ServerSocket serverSocket) {
		this.serverSocket = serverSocket;
	}

	/**
	 * @return Returns the listenerPort.
	 */
	public int getListenerPort() {
		return listenerPort;
	}

	/**
	 * @param listenerPort
	 *            The listenerPort to set.
	 */
	public void setListenerPort(int listenerPort) {
		this.listenerPort = listenerPort;
	}

	public void clearConnections() {
		if (this.serverSocket != null) {
			try {
				this.serverSocket.close();
			} catch (IOException e) {
			}
		}

	}

}
