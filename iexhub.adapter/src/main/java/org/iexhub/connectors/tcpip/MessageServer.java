
package org.iexhub.connectors.tcpip;

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
 *******************************************************************************/

public class MessageServer extends Thread {
	static {
		ConnectorProperties.initProperties(System.getProperty("PROPERTIES"));
	}

	static Logger logger = Logger.getLogger(MessageServer.class);

	public MessageServer() {

	}

	public void run() {
		runThread();
	}

	private void runThread() {
		process();
	}

	private void process() {

		// Start listening
		int listenerPort = Integer.parseInt(ConnectorProperties.getInstance()
				.getProperty("/properties/socket/port"));
		ListenerThread thread = new ListenerThread( listenerPort);
		thread.start();
		// Now wait for the threads to complete...
		try {
			thread.join();
		} catch (IllegalMonitorStateException e) {
			System.err.println("Serious threading problem: " + e.toString());
		} catch (InterruptedException e) {
			logger
					.error("Shutting down listener thread, waiting for shutdown to complete...");
			thread.interrupt();
			try {
				thread.join();
				logger.info("Listener thread shut down");
			} catch (Exception ex) {
				logger.error("Exception encountered in thread.join()"
						+ ex.getLocalizedMessage());
				ex.printStackTrace();
			}
		}

	}

}
