package org.iexhub.connectors.database;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.iexhub.connectors.tcpip.common.ConnectorProperties;

import com.eversolve.Medi7.M7Composite;
import com.eversolve.Medi7.M7DefinitionFile;
import com.eversolve.Medi7.M7Exception;
import com.eversolve.Medi7.M7Message;
import com.eversolve.Medi7.M7Repeat;
import com.eversolve.Medi7.datatypes.M7DateTime;

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
public abstract class BasePersistence {

	static Logger logger = Logger.getLogger(BasePersistence.class);

	/**
	 * Name of the status column which indicates if something was sent or not
	 */

	/**
	 * Database connection
	 */
	String connectionStr = ConnectorProperties.getInstance().getProperty("/properties/jdbc/connection");

	static M7DefinitionFile mdf = null;

	static M7DefinitionFile adf = null;

	static Connection dbConnection = null;

	ResultSet rs = null;

	static {
		String driverClass = ConnectorProperties.getInstance().getProperty("/properties/jdbc/driver");
		try {
			// This will ensure that the
			// "jdbc:sqlserver://" URL prefix is
			// handled by the 2005 version of the JDBC driver and the
			// "jdbc:microsoft:sqlserver://"
			// URL prefix is handled by the 2000 version of the JDBC driver
			// Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver"); //
			// 2005
			// version
			// Class.forName("com.microsoft.jdbc.sqlserver.SQLServerDriver"); //
			// 2000
			// version
			// The newInstance() call is a work around for some broken Java
			// implementations
			// Class.forName("com.mysql.jdbc.Driver").newInstance();
			//
			Class.forName(driverClass);

		} catch (Exception ex) {
			logger.error(ex);
		}

	}

	/**
	 * Database mapping and persistence constructor
	 * 
	 * @param messageDefinitionFileName
	 * @param responseMessageDefinitionFileName
	 */
	public BasePersistence(String messageDefinitionFileName, String responseMessageDefinitionFileName) {
		super();
		try {
			mdf = new M7DefinitionFile(messageDefinitionFileName);
		} catch (M7Exception e1) {
			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		} catch (IOException e1) {
			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		}

		try {
			adf = new M7DefinitionFile(responseMessageDefinitionFileName);
		} catch (M7Exception e1) {
			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		} catch (IOException e1) {
			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		}

		connect();
	}

	/**
	 * Connect to the database
	 */
	public void connect() {
		logger.info("Connecting to '" + connectionStr + "'");
		while (dbConnection == null) {
			try {
				dbConnection = DriverManager.getConnection(connectionStr.trim());
				logger.info("Successfully connected to the db");

			} catch (SQLException se) {
				// handle any errors
				/*
				 * logger.error("Unable to connect to database'" + connectionStr
				 * + "'"); logger.error("SQLException: " + ex.getMessage());
				 * logger.error("SQLState: " + ex.getSQLState()); logger.error(
				 * "VendorError: " + ex.getErrorCode()); logger.error("Cause: "
				 * + ex.getCause());
				 */
				do {
					logger.error("SQL STATE: " + se.getSQLState());
					logger.error("ERROR CODE: " + se.getErrorCode());
					logger.error("MESSAGE: " + se.getMessage());
					se = se.getNextException();
				} while (se != null);
			}
			if (dbConnection == null) {
				logger.debug("Retrying the database connection...");
			}
		}
	}

	/**
	 * Clears the connection in the case of shutdown
	 */
	public void clearConnection() {
		try {
			if (rs != null) {
				this.rs.close();
			}
			if (dbConnection != null) {
				dbConnection.close();
				dbConnection = null;
			}
		} catch (SQLException e) {
			logger.debug(e.getLocalizedMessage());
		}
	}

	/**
	 * Reads the field values and places them in a hash map for later use in
	 * persistence operations
	 * 
	 * @param msg
	 *            inbound message
	 * @param columnToFieldMap
	 *            - Column name to Column object map column to field hash map
	 * @param loop
	 *            path for repeating segment
	 * @return rows vector of column/value hash maps
	 */
	public Vector<HashMap<String, Column>> readMessageToRows(M7Message msg, HashMap<String, Column> columnToFieldMap,
			String loopMessagePath) {

		// column information and possible values
		Collection<String> columnNames = columnToFieldMap.keySet();
		Vector<HashMap<String, Column>> rows = new Vector<HashMap<String, Column>>();
		StringBuffer errorBuffer = new StringBuffer();
		M7Repeat loop = null;
		try {
			loop = msg.getRepeat(loopMessagePath);
		} catch (M7Exception e1) {
			e1.printStackTrace();
			logger.error("Loop definition error: missing '" + loopMessagePath + "'");
			return null;
		}
		for (int i = 0; i < loop.getChildCount(); i++) {
			M7Composite segment = null;
			try {
				segment = loop.getItemAt(i);
			} catch (M7Exception e1) {
				logger.error("Loop definition error: missing child lements for'" + loopMessagePath + "'");
				e1.printStackTrace();
				return null;
			}
			HashMap<String, Column> rowWithValues = new HashMap<String, Column>();
			Iterator<String> columnNamesIterator = columnNames.iterator();
			boolean rowCompleted = true;
			while (columnNamesIterator.hasNext() && rowCompleted ) {
				String columnName = (String) columnNamesIterator.next();
				//copy the column/field mapping info
				Column newColumnObject = new Column((Column) columnToFieldMap.get(columnName));
				logger.debug("Validate column name:" + columnName + ", corresponds to column:" + newColumnObject.toString());
				String messageFieldPath = null;
				try {
					String value = null; // from the message
					messageFieldPath = newColumnObject.getFieldName();
					if (messageFieldPath.contains("$")) { // auto-generated date/time
						if (messageFieldPath.equals("$datetime")) {
							value = new M7DateTime().getSQLFormat();
						}
						newColumnObject.setValue(value);
						rowWithValues.put(newColumnObject.getColumnName(), newColumnObject);
						logger.debug(newColumnObject.getColumnName() + "=" + value);
					} else if (messageFieldPath.contains("=")) { // fixed value
						value = messageFieldPath.substring(1);
						newColumnObject.setValue(value);
						rowWithValues.put(newColumnObject.getColumnName(), newColumnObject);
						logger.debug(newColumnObject.getColumnName() + "=" + value);
					} else {//message field processing
						M7Composite field = null;
						String relativePath = null;
						if (!messageFieldPath.contains("[x]")) {
							field = msg.getChild(messageFieldPath);
						} else {
							int pos = messageFieldPath.indexOf("x");
							relativePath = messageFieldPath.substring(pos + 3);					
							logger.debug("Truncated: " + messageFieldPath + " to: " + relativePath);
							//if(segment.getType()== M7Composite.eM7Segment)						
							field = segment.getChild(relativePath);						

						}
						if (field != null) {
							String structureType = field.getDefinition().getStructureType();
							if ((field.getType() == M7Composite.eM7Field)
									&& (structureType.equals("DTM") || structureType.equals("TS"))) {

								{
									M7DateTime dt = new M7DateTime(field.toString());
									value = dt.getSQLFormat().toString();
								}
							} else {
								value = field.toString();
							}
							// look for translations, if they exist
							String mappingPath = getColumnPath() + "[@key='" + columnName + "']/map/code";
							HashMap<String, String> valueMap = ConnectorProperties.getInstance()
									.getLookupTables(mappingPath);

							if ((valueMap != null) && (valueMap.size() > 0)) {
								logger.debug("Replace '" + value + "' with '" + valueMap.get(value) + "'");
								value = valueMap.get(value);
							}
							if ((valueMap != null) && (valueMap.size() > 0) && (value == null)) {
								String warning = "Skipped row mapping for: " + segment;
								logger.warn(warning);
								errorBuffer.append(warning);
								rowCompleted = false;
								break;
							}
							// add the value to the message map
							newColumnObject.setValue(value);
							rowWithValues.put(newColumnObject.getColumnName(), newColumnObject);
							logger.debug(newColumnObject.getColumnName() + "=" + value);
						} else {//the field is null
							logger.warn("Skipping row; field '"+ messageFieldPath +"' is null; in"+segment.toString());							
							//errorBuffer.append(error);
							rowCompleted = false;
							break;
						}
					}//message field processed

				} catch (M7Exception e) {
					logger.error(e.getLocalizedMessage());
					errorBuffer.append(e.getLocalizedMessage());
				}
			}
			// add new row
			if (rowCompleted) {
				logger.debug("Addigng a row: " + rowWithValues);
				rows.addElement(rowWithValues);
			}
		}
		if (errorBuffer.length() > 0) {
			logger.info(errorBuffer);
		}
		return rows;

	}

	/**
	 * Method refreshConnection: checks the db connection and reconnects, if
	 * needed
	 */
	public synchronized void refreshConnection() {
		// reconnect if connection lost
		try {
			if (dbConnection == null) {
				logger.debug("Reconnecting the null db connection");
				connect();
			} else if (dbConnection.isClosed()) {
				logger.debug("Reconnecting the closed db connection");
				clearConnection();
				connect();
			}
		} catch (SQLException e) {
			logger.error("Reconnecting due to SQLException: " + e.getMessage());
			clearConnection();
			connect();
		}
	}

	/**
	 * Method findRows verifies that a record that matches the primary key
	 * values; it returns -1 if the query fails due to SQL errors
	 * 
	 * @param tableName
	 * @param columnsClause
	 * @param primaryKeyValues
	 * @return number of matching records
	 */
	public int findRows(String tableName, String columnsClause, String primaryKeyValues) {
		Statement stmt = null;
		int records = 0;
		String findExistingRecordStr = "SELECT " + columnsClause + " FROM " + tableName + " WHERE (" + primaryKeyValues
				+ ")";
		logger.debug("Finding records:" + findExistingRecordStr);
		this.refreshConnection();
		try {
			stmt = dbConnection.createStatement();
			// ResultSet.TYPE_SCROLL_INSENSITIVE,
			// ResultSet.CONCUR_UPDATABLE);
			rs = stmt.executeQuery(findExistingRecordStr);
			while (rs.next()) {
				records++;
			}
			rs.close();
		} catch (SQLException ex) {
			logger.error(
					"SQLException: " + ex.getMessage() + " attempting to execute:\n\t '" + findExistingRecordStr + "'");
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());
			return -1;
		}
		return records;
	}

	/**
	 * Method insert row
	 * 
	 * @param tableName
	 * @param columns
	 * @param values
	 * @return int number of rows
	 */
	public int insertRow(String tableName, String columns, String values) {
		Statement stmt = null;
		int result = 0;
		String insertStr = "INSERT INTO " + tableName + " (" + columns + ") VALUES (" + values + ")";
		logger.debug("Inserting row: " + insertStr);
		this.refreshConnection();
		try {
			stmt = dbConnection.createStatement();
			// ResultSet.TYPE_SCROLL_INSENSITIVE,
			// ResultSet.CONCUR_UPDATABLE);
			result = stmt.executeUpdate(insertStr);
		} catch (SQLException ex) {
			logger.error("SQLException: " + ex.getMessage() + " attempting to execute:\n\t '" + insertStr + "'");
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());

		}
		return result;
	}

	/**
	 * Method update one row
	 * 
	 * @param tableName
	 * @param columns
	 * @param values
	 * @return updated row count
	 */
	public int updateRow(String tableName, String valuesSetClause, String whereClause) {
		Statement stmt = null;
		int result = 0;
		String updateStr = "UPDATE " + tableName + " SET " + valuesSetClause + " WHERE (" + whereClause + ")";
		logger.debug("Updating records:" + updateStr);
		this.refreshConnection();
		try {
			stmt = dbConnection.createStatement();
			// ResultSet.TYPE_SCROLL_INSENSITIVE,
			// ResultSet.CONCUR_UPDATABLE);
			result = stmt.executeUpdate(updateStr);
		} catch (SQLException ex) {
			logger.error("SQLException: " + ex.getMessage() + " attempting to execute:\n\t '" + updateStr + "'");
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());

		}
		return result;
	}

	/**
	 * Reads the column values for this row and places them in a map. The map is
	 * added to the vector we build for this purpose.
	 * 
	 * @param row
	 * @return field-column hash map
	 */
	public HashMap<String, String> readRow(ResultSet row, HashMap<String, Column> fieldColumnMap) {
		// Set keySet = fieldColumnMap.keySet();
		// column names
		Collection<Column> valueSet = fieldColumnMap.values();
		Iterator<Column> iterator = valueSet.iterator();
		HashMap<String, String> rowMap = new HashMap<String, String>();
		while (iterator.hasNext()) {
			Column column = (Column) iterator.next();
			String value = null;
			try {
				value = row.getString(column.getColumnName());
				logger.debug(column.getColumnName() + "=" + value);
				rowMap.put(column.getColumnName(), value);
				logger.debug("Update column");
				column.setValue(value);
			} catch (SQLException ex) {
				ex.printStackTrace();
				logger.error("SQLException: " + ex.getMessage() + " attempting to execute:\n\t '"
						+ column.getColumnName() + "=" + value + "'");
				logger.error("SQLException: " + ex.getMessage());
				logger.error("SQLState: " + ex.getSQLState());
				logger.error("VendorError: " + ex.getErrorCode());
				System.exit(0);
			}
		}
		return rowMap;

	}

	/**
	 * getColumnPath returns the path - in the properties file - there the
	 * column mappings are located. This path can be used with the key(field
	 * path) and value (table column)
	 * 
	 * @return string containing the path
	 */
	public abstract String getColumnPath();

}
