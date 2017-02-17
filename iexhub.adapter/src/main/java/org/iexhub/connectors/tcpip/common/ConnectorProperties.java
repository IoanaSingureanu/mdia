

package org.iexhub.connectors.tcpip.common;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import javax.swing.JOptionPane;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.TransformerException;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;
import org.apache.xpath.XPathAPI;
import org.iexhub.connectors.database.Column;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

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
public class ConnectorProperties {
	static Logger logger = Logger.getLogger(ConnectorProperties.class);

	/** The singleton. */
	private static ConnectorProperties instance = null;

	/** The document containing the properties. */
	private final Document doc;

	/** Root tag for our document. */
	static String DOC_ROOT = "doc-root";

	/**
	 * This factory method initializes the singleton instance, retrieving
	 * properties from the files specified in the array. This method should be
	 * called once at process startup. Subsequent requests to get the properties
	 * instance should use the version of <b>getInstance </b> that takes no
	 * arguments.
	 * 
	 * @param files
	 *            A list of fully-qualified file names.
	 * @return A new BASProperties instance.
	 * 
	 * @throws FileNotFoundException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	public static ConnectorProperties getInstance(String[] files)
			throws FileNotFoundException, ParserConfigurationException,
			SAXException, IOException {
		if (instance == null) {
			instance = new ConnectorProperties(files);
		}
		return instance;
	}

	/**
	 * This method is a convenience method that can be used when there is only a
	 * single properties file to load. It simply creates an array with one file
	 * in it and calls the <b>getInstance </b> method that takes a list of
	 * files.
	 * 
	 * @param file
	 *            A fully-qualified file name containing properties.
	 * @return A new XMLProperties instance.
	 * 
	 * @throws FileNotFoundException
	 * @throws ParserConfigurationException
	 * @throws SAXException
	 * @throws IOException
	 */
	public static ConnectorProperties getInstance(String file)
			throws FileNotFoundException, ParserConfigurationException,
			SAXException, IOException {
		String[] files = { file };
		return getInstance(files);
	}

	/**
	 * This method returns the singleton BASProperties instance. The singleton
	 * should be initialized by calling the <b>getInstance </b> method that
	 * takes a list of files.
	 * 
	 * @return The XMLProperties singleton.
	 */
	public static ConnectorProperties getInstance() {
		return instance;
	}

	/**
	 * This constructor creates the document that we'll use to store the
	 * properties. We create a master document and add each individual document
	 * (as specified by the file names in the string array argument).
	 * 
	 * @param files
	 *            The fully-qualified properties file names.
	 * 
	 * @throws ParserConfigurationException
	 * @throws FileNotFoundException
	 * @throws SAXException
	 * @throws IOException
	 */
	private ConnectorProperties(String[] files)
			throws ParserConfigurationException, FileNotFoundException,
			SAXException, IOException {
		// setup the parser
		DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
		dbf.setIgnoringElementContentWhitespace(true);
		DocumentBuilder db = dbf.newDocumentBuilder();
		// create a new document that will contain the individual docs
		doc = db.newDocument();
		doc.appendChild(doc.createElement(DOC_ROOT));
		// parse the individual docs and add them to the master
		for (int i = 0; i < files.length; i++) {
			Document subDoc = db
					.parse(new InputSource(new FileReader(files[i])));
			doc.getDocumentElement().appendChild(
					doc.importNode(subDoc.getDocumentElement(), true));
		}
	}

	/**
	 * This method retrieves the value for a property. The name is an absolute
	 * XPath expression. For example, to lookup the value for the property
	 * specified by this XML:
	 * 
	 * <props><main><foo>1344 </foo> ...
	 * 
	 * you would pass the name: /properties/main/property
	 * 
	 * @param name
	 *            An absolute XPath expression that identifies the property
	 *            name.
	 * 
	 * @return The string representation of the value, or null if the name
	 *         doesn't exist or if there is no value in the XML.
	 */
	public String getProperty(String name) {
		String result = null;
		NodeList nl = null;
		try {
			// execute the XPath query - note that we prepend the root
			// tag of the document we created to hold the individual docs
			nl = XPathAPI.selectNodeList(doc, "/" + DOC_ROOT + name); //$NON-NLS-1$
		} catch (TransformerException e) {
			e.printStackTrace();
		}
		// if we found a node and it has a value, return it
		if (nl != null && nl.getLength() > 0) {
			Node val = nl.item(0).getFirstChild();
			if (val != null) {
				result = val.getNodeValue();
				if (result != null) {
					result = result.trim();
				}
			}
		}
		logger.debug("property '" + name + "': " + result);
		return result;
	}

	/**
	 * Retrieves database key-value pairs from the configuration file
	 * 
	 * @param propertyPath
	 * @return database to message map hash map
	 */
	public HashMap<String, Column> getColumnToFieldMaps(String propertyPath) {
		HashMap<String, Column> map = new HashMap<String, Column>();

		NodeList nl = null;
		try {
			// execute the XPath query - note that we prepend the root
			// tag of the document we created to hold the individual docs
			nl = XPathAPI.selectNodeList(doc, "/" + DOC_ROOT + propertyPath); //$NON-NLS-1$
		} catch (TransformerException e) {
			e.printStackTrace();
			logger.error(e.getLocalizedMessage());
			return map;
		}
		// if we found a node and it has a value, return it
		String columnName  = null;
		String messageField = null;
		Integer maxlen = null;
		Boolean primaryKey = null;
		Boolean updateable = null;
		Integer multiplicity = null;
		for (int i = 0; ((nl != null) && (i < nl.getLength())); i++) {
			Node val = nl.item(i);
			if (val != null) {
				Column col = new Column();
				messageField = val.getAttributes().getNamedItem("value")
				.getNodeValue().trim();
				columnName= val.getAttributes().getNamedItem("key").getNodeValue()
				.trim();
				if(val.getAttributes().getNamedItem("multiplicity")!=null)
				{
					multiplicity= new Integer(val.getAttributes().getNamedItem("multiplicity").getNodeValue()
					.trim());
					col.setMultiplicity(multiplicity.intValue());
				}
				if(val.getAttributes().getNamedItem("maxlength")!=null)
				{
					maxlen= new Integer(val.getAttributes().getNamedItem("maxlength").getNodeValue()
							.trim());
					col.setMaxLength(maxlen.intValue());
				}	
				if(val.getAttributes().getNamedItem("pk")!=null)
				{
					primaryKey= new Boolean(val.getAttributes().getNamedItem("pk").getNodeValue()
							.trim());
					col.setPrimaryKey(primaryKey.booleanValue());
				}
				if(val.getAttributes().getNamedItem("update")!=null)
				{
					updateable= new Boolean(val.getAttributes().getNamedItem("update").getNodeValue()
							.trim());
					col.setUpdateable(updateable.booleanValue());
				}
				col.setColumnName(columnName);
				col.setFieldName(messageField);	
				
				map.put(columnName, col); 
			}
		}
		return map;
	}

	/**
	 * Retrieves lookup table key-value pairs from the configuration file
	 * 
	 * @param propertyPath
	 * @return lookup table for a field
	 */
	public HashMap<String, String> getLookupTables(String propertyPath) {
		HashMap<String, String> map = new HashMap<String, String >();

		NodeList nl = null;
		try {
			// execute the XPath query - note that we prepend the root
			// tag of the document we created to hold the individual docs
			nl = XPathAPI.selectNodeList(doc, "/" + DOC_ROOT + propertyPath); //$NON-NLS-1$
		} catch (TransformerException e) {
			e.printStackTrace();
			logger.error(e.getLocalizedMessage());
			return map;
		}
		// if we found a node and it has a value, return it
		String key  = null;
		String value = null;
		for (int i = 0; ((nl != null) && (i < nl.getLength())); i++) {
			Node val = nl.item(i);
			if (val != null) {
				value = val.getAttributes().getNamedItem("value")
						.getNodeValue().trim();
				key= val.getAttributes().getNamedItem("key")
						.getNodeValue().trim();
				map.put(key, value); 
			}
		}
		return map;
	}
	/**
	 * This method behaves like the above method, except that a default value is
	 * returned if the property doesn't exist or does not have a value in the
	 * properties file.
	 * 
	 * @param name
	 *            The name of the property to retrieve.
	 * @param defaultValue
	 *            A default value.
	 * @return The property value if available, otherwise the default value.
	 */
	public String getProperty(String name, String defaultValue) {
		String val = getProperty(name);
		return val != null ? val : defaultValue;
	}

	/**
	 * This method allows the caller to query for tags that are not known a
	 * priori. For example, we can configure any number of database connection
	 * pools in the properties file. A caller can query for the list of
	 * connection pool tags and then iterate over them, retrieving individual
	 * properties for each pool.
	 * 
	 * @param path
	 *            An XPath expression to execute.
	 * @return An array of Strings that represent the node names that match the
	 *         path expression.
	 */
	protected String[] resolveNames(String path) {
		// the resulting tag names are stored in a Map so that duplicates
		// will be filtered
		Map<String, String> names = new HashMap<String, String>();
		NodeList nl = null;
		try {
			// execute the XPath query - note that we prepend the root
			// tag of the document we created to hold the individual docs
			nl = XPathAPI.selectNodeList(doc, "/" + DOC_ROOT + path); //$NON-NLS-1$
		} catch (TransformerException e) {
			// ignore - we'll return an empty list down below
		}
		// add the names of the found nodes to the list
		if (nl != null) {
			for (int i = 0; i < nl.getLength(); i++) {
				// insert the name in the map - we don't care about the value
				names.put(nl.item(i).getNodeName(), null);
			}
		}
		// return the map's key set as an array
		return names.keySet().toArray(new String[0]);
	}

	/**
	 * This method returns the node (cast to an Element) for the Log4j component
	 * of the document. We do this because the Log4j DOMConfigurator needs the
	 * root logging node from the document.
	 * 
	 * @return The node from the document that contains the Log4j information.
	 */
	private Element getLogger() {
		NodeList nl = doc.getElementsByTagName("log4j:configuration"); //$NON-NLS-1$
		return (nl != null && nl.getLength() > 0 ? (Element) nl.item(0) : null);
	}

	/**
	 * Initializes the default properties to a propertyFile
	 * 
	 * @param propertyFile
	 *            to load properties from
	 */
	public static void initProperties(String propertyFile) {
		//
		// get the properties from the property file
		//
		if (propertyFile == null) {
			String usageError = "Missing property file argument \n e.g. -DPROPERTIES='properties\\core.xml'  \nVerify VM arguments";
			System.err.println(usageError);
			JOptionPane.showMessageDialog(null, usageError,
					"Command-line Argument Error", JOptionPane.ERROR_MESSAGE); //$NON-NLS-1$
			System.exit(0);
		}
		try {
			ConnectorProperties props = ConnectorProperties
					.getInstance(propertyFile);
			try {
				// configure Log4j
				DOMConfigurator.configure(props.getLogger());
			} catch (Exception e) {
				// error, do the default initialization
				BasicConfigurator.configure();
			}
			logger = Logger.getLogger(ConnectorProperties.class);

		} catch (Exception e) {
			System.out
					.println("Unable to read properties from:" + propertyFile + "; due to: " + e); //$NON-NLS-1$ //$NON-NLS-2$
		}
	}
}