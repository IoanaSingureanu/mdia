package org.iexhub.connectors.database;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.iexhub.connectors.tcpip.common.ConnectorProperties;

import com.eversolve.Medi7.M7Message;
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
public class DatabaseMap extends BasePersistence {

	static Logger logger = Logger.getLogger(DatabaseMap.class);

	/**
	 * Public Constants
	 */

	public static String TABLE_NAME = ConnectorProperties.getInstance()
			.getProperty("/properties/message/database/table/name");

	public static String MSG_DEF_FILE_NAME = ConnectorProperties.getInstance()
			.getProperty("/properties/message/definition");

	public static String ACK_DEF_FILE_NAME = ConnectorProperties.getInstance()
			.getProperty("/properties/message/ack/file");

	public static String LOOP_PATH = ConnectorProperties.getInstance().getProperty("/properties/message/loop");

	public static String DEFAULT_COL_PATH = "/properties/message/database/table/map/column";

	/**
	 * tableToMessageMap of type 'HashMap'
	 */
	HashMap<String, Column> tableToMessageMap;

	/**
	 * messageToTableMap of type 'HashMap'
	 */
	HashMap<String, Column> messageToTableMap = ConnectorProperties.getInstance()
			.getColumnToFieldMaps(DEFAULT_COL_PATH);

	public HashMap<String, Column> getMessageToTable() {
		return messageToTableMap;
	}

	public void setMessageToTable(HashMap<String, Column> messageToTable) {
		this.messageToTableMap = messageToTable;
	}

	public HashMap<String, Column> getTableToMessage() {
		return tableToMessageMap;
	}

	public void setTableToMessage(HashMap<String, Column> tableToMessage) {
		this.tableToMessageMap = tableToMessage;
	}

	/**
	 * Method main
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

	/**
	 * Database mapping and persistence constructor
	 * 
	 * @param messageDefinitionFileName
	 * @param responseMessageDefinitionFileName
	 */
	public DatabaseMap(String messageDefinitionFileName, String responseMessageDefinitionFileName) {
		super(messageDefinitionFileName, responseMessageDefinitionFileName);

		// register the shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				clearConnection();
			}
		});
	}

	/**
	 * Default constructor
	 */
	public DatabaseMap() {
		this(MSG_DEF_FILE_NAME, ACK_DEF_FILE_NAME);
	}

	/**
	 * Persists the message contents
	 * 
	 * @param msg
	 *            M7Message - null if parsing fails
	 * @param errorCode
	 *            - used if the msg is null
	 * @param errorMessage
	 *            - used if the msg is null
	 */
	public boolean persistMessageContent(M7Message msg, StringBuffer errorMessage) {

		Vector<HashMap<String, Column>> rows = new Vector<HashMap<String, Column>>();
		if (msg == null) {
			// nothing to persist
			logger.debug("Nothing to persist, message content is null");
			errorMessage.append("Null message");
			return false;
		} else {
			logger.debug("Message contents: \n" + msg.debug());
			rows = this.readMessageToRows(msg, this.messageToTableMap, LOOP_PATH);
			if (rows.size() == 0) {
				//errorMessage.append("Nothing to persist in this message: \n" + msg.toString());
				logger.info("Nothing to persist due to mapping or configuration data settings. Return success.");
				return true;
			}
		}

		return persistRows(rows, TABLE_NAME);

	}

	@Override
	public String getColumnPath() {
		return DEFAULT_COL_PATH;
	}

	/**
	 * getMessageToTableMap
	 * 
	 * @return
	 */
	public HashMap<String, Column> getMessageToTableMap() {
		return messageToTableMap;
	}

	/**
	 * getTableToMessageMap
	 * 
	 * @return
	 */
	public HashMap<String, Column> getTableToMessageMap() {
		return tableToMessageMap;
	}

	/**
	 * Insert a new row into the staging table using the contents of the inbound
	 * message hash map
	 * 
	 * @param messageColumnMap
	 *            contains the column (key) and the value from the inbound
	 *            message
	 */
	public synchronized boolean persistRows(Vector<HashMap<String, Column>> columnsValuesMapVector, String tableName) {

		boolean insertedRowFlag = false;
		for (int k = 0; k < columnsValuesMapVector.size(); k++) {

			HashMap<String, Column> messageColumnMap = columnsValuesMapVector.get(k);
			StringBuffer columnsClause = new StringBuffer();
			StringBuffer valuesClause = new StringBuffer();
			StringBuffer whereClause = new StringBuffer();
			StringBuffer updateValuesClause = new StringBuffer();
			// iterate columns
			Iterator<Column> columns = messageColumnMap.values().iterator();
			logger.debug("Vital type column info: " + messageColumnMap.get("VITAL_TYPE"));

			while (columns.hasNext()) {
				if (columnsClause.length() > 0)
					columnsClause.append(",");

				if (valuesClause.length() > 0)
					valuesClause.append(",");
				Column col = columns.next();
				String columnKey = col.getColumnName();
				String columnValue = col.getValue();
				valuesClause.append("'" + escapeChars(columnValue) + "'");
				columnsClause.append(columnKey);
				// update the non-primary columns
				if (col.isUpdateable()) {
					if (updateValuesClause.length() > 0)
						updateValuesClause.append(",");
					updateValuesClause.append(columnKey + "='" + escapeChars(columnValue) + "'");
				}
				// unique data elements using for
				if (col.isPrimaryKey()) {
					if (whereClause.length() > 0)
						whereClause.append(" AND ");
					whereClause.append(columnKey + "='" + escapeChars(columnValue) + "'");
				}
			}
			int rowsFound = this.findRows(tableName, columnsClause.toString(), whereClause.toString());
			if (rowsFound > 0) {
				if (this.updateRow(tableName, updateValuesClause.toString(), whereClause.toString()) == 1)
					insertedRowFlag = true;
			} else if (rowsFound == 0) {
				if (this.insertRow(tableName, columnsClause.toString(), valuesClause.toString()) == 1)
					insertedRowFlag = true;
			}
		}
		return insertedRowFlag;
	}

	private String escapeChars(String value) {
		return value.replace('\'', ' ').replace('"', ' ');

	}

	/**
	 * updateRow updates an existing row into the staging table using the
	 * contents of the inbound message hash map
	 * 
	 * @param messageColumnMap
	 *            contains the column (key) and the value from the inbound
	 *            message
	 * @param tableName
	 * @param column
	 * @param value
	 */
	public synchronized boolean updateRow(HashMap<String, Column> messageColumnMap, String tableName, String column,
			String value) {
		Statement stmt = null;
		String stmtStr = null;
		boolean updatedRowFlag = false;
		refreshConnection();

		try {
			stmt = dbConnection.createStatement();
			// ResultSet.TYPE_SCROLL_INSENSITIVE,
			// ResultSet.CONCUR_UPDATABLE);

		} catch (SQLException ex) {
			logger.error("SQLException: " + ex.getMessage());
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());

		}
		if (stmt != null) {
			try {

				stmtStr = "UPDATE " + tableName + " SET ";
				// iterate columns
				Collection<String> columns = messageColumnMap.keySet();
				Iterator<String> colIterator = columns.iterator();
				while (colIterator.hasNext()) {
					String colName = (String) colIterator.next();
					stmtStr += (colName + "='");
					stmtStr += messageColumnMap.get(colName);
					stmtStr += "', ";

				}
				// updated field
				stmtStr += "updated ='" + (new M7DateTime().getSQLFormat()) + "'";

				stmtStr += " WHERE " + column + "='" + this.escapeChars(value) + "'";

				logger.debug(stmtStr);
				stmt.executeUpdate(stmtStr);
				updatedRowFlag = true;

			} catch (SQLException ex) {

				logger.error("SQLException: " + ex.getMessage());
				logger.error("SQLState: " + ex.getSQLState());
				logger.error("VendorError: " + ex.getErrorCode());

			}
			try {
				stmt.close();
			} catch (SQLException e) {
				logger.error(e.getLocalizedMessage());
				e.printStackTrace();
			}
		}
		return updatedRowFlag;

	}

}
