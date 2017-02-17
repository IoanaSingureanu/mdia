
package org.iexhub.connectors.tcpip.common;

import org.apache.log4j.Logger;

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

public class GCthread extends Thread {

	static Logger logger = Logger.getLogger(GCthread.class);

	private int sleepInterval = 600000; // Default of 10 minutes

	public GCthread(int sleepInterval) {
		this.setSleepInterval(sleepInterval);

	}

	public void run() {
		runThread();
	}

	private void runThread() {
		try {
			while (!isInterrupted()) {
				sleep(sleepInterval);

				long startTime = System.currentTimeMillis();
				long initialFreeMemory = Runtime.getRuntime().freeMemory();
				logger
						.debug("Starting JVM garbage collector; free memory before collection="
								+ Runtime.getRuntime().freeMemory()
								+ " bytes; total memory in use="
								+ Runtime.getRuntime().totalMemory() + " bytes");

				System.gc();

				logger
						.debug("Garbage collection finished; elapsed time="
								+ (System.currentTimeMillis() - startTime)
								+ " ms, memory freed="
								+ (Runtime.getRuntime().freeMemory() - initialFreeMemory)
								+ " bytes");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * @return Returns the sleepInterval.
	 */
	public int getSleepInterval() {
		return sleepInterval;
	}

	/**
	 * @param sleepInterval
	 *            The sleepInterval to set.
	 */
	public void setSleepInterval(int sleepInterval) {
		this.sleepInterval = sleepInterval;
	}
}