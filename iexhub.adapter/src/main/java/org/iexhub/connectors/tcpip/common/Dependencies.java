
package org.iexhub.connectors.tcpip.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;

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
public class Dependencies {
	static {
		ConnectorProperties.initProperties(System.getProperty("PROPERTIES"));

	}

	static Logger logger = Logger.getLogger(Dependencies.class);

	String sourceFileName;

	String destinationDirectory;

	public String getDestinationDirectory() {
		return destinationDirectory;
	}

	public void setDestinationDirectory(String destinationDirectory) {
		this.destinationDirectory = destinationDirectory;
	}

	public String getSourceFileName() {
		return sourceFileName;
	}

	public void setSourceFileName(String sourceFileName) {
		this.sourceFileName = sourceFileName;
	}

	public Dependencies() {
		;
	}

	/**
	 * Copies src file to dst file. If the dst file does not exist, it is
	 * created
	 * 
	 * @param inFile
	 * @param outFile
	 * @throws IOException
	 */

	// void copy(File inFile, File outFile) throws IOException {
	// InputStream in = new FileInputStream(inFile);
	// OutputStream out = new FileOutputStream(outFile);
	//    
	// // Transfer bytes from in to out
	// byte[] buf = new byte[4096];
	// int len;
	// while ((len = in.read(buf)) > 0) {
	// out.write(buf, 0, len);
	// }
	// in.close();
	// out.close();
	// }
	//    
	public void copyFile(File inFile, File outFile) throws Exception {
		FileInputStream fis = new FileInputStream(inFile);
		FileOutputStream fos = new FileOutputStream(outFile);
		byte[] buf = new byte[16348];
		int i = 0;
		while ((i = fis.read(buf)) != -1) {
			fos.write(buf, 0, i);
		}
		fis.close();
		fos.close();
	}

	public void load() {
		RandomAccessFile fileList = null;
		try {
			fileList = new RandomAccessFile(this.getSourceFileName(), "r");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			System.err.println("Unable to open the dependency list "
					+ e.getLocalizedMessage());
			System.exit(0);
		}
		String dependencyFileName = null;
		try {
			dependencyFileName = fileList.readLine();
		} catch (IOException e) {
			e.printStackTrace();
			System.err.println("Unable to open the dependency list "
					+ e.getLocalizedMessage());
			System.exit(0);
		}
		//File destDir  = new File(this.getDestinationDirectory());
		while (dependencyFileName != null) {

			File file = new File(dependencyFileName);
					
		    File dest = new File(this.getDestinationDirectory(),file.getName());
		    try {
				dest.createNewFile();
			} catch (IOException e2) {
				logger.error(e2);
				e2.printStackTrace();
			}
		    try {
		    	logger.debug("Copy '"+file.getAbsolutePath() +"' to '"+dest.getAbsolutePath()+"'");
				copyFile(file, dest);
			} catch (Exception e1) {
				logger.error(e1);
				e1.printStackTrace();
			}

			// get next line
			try {
				dependencyFileName = fileList.readLine();
			} catch (IOException e) {
				logger.error(e);
				e.printStackTrace();
				break;
			}
			
		}

	}

	/**
	 * Get the dependencies and copy them to a target directory
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		Dependencies deps = new Dependencies();

		if (args.length == 2) {

			deps.setSourceFileName(args[0]);
			// destination directory
			deps.setDestinationDirectory(args[1]);

		} else {
			System.err
					.println("Exactly two command-line parameter supported: file containing the list of dependency dlls and the source directory");
			System.exit(0);

		}

		logger.info("Loading dependencies from '" + deps.getSourceFileName()
				+ "' to dir '" + deps.getDestinationDirectory() + "'");

		deps.load();
	}

}
