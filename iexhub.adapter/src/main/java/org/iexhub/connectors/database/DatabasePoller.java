
package org.iexhub.connectors.database;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Vector;

import org.apache.log4j.Logger;
import org.iexhub.connectors.tcpip.common.ConnectorProperties;

import com.eversolve.Medi7.M7Composite;
import com.eversolve.Medi7.M7Exception;
import com.eversolve.Medi7.M7Field;
import com.eversolve.Medi7.M7Message;
import com.eversolve.Medi7.M7Repeat;
import com.eversolve.Medi7.datatypes.M7Date;
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
public class DatabasePoller extends BasePersistence {
	static Logger logger = Logger.getLogger(DatabasePoller.class);

	/**
	 * Constants
	 */
	public static String MSG_DEF_FILE_NAME = ConnectorProperties.getInstance()
			.getProperty(
					"/properties/message/"
							+ ConnectorProperties.getInstance().getProperty(
									"/properties/database/message/file"));

	public static String ACK_DEF_FILE_NAME = ConnectorProperties.getInstance()
			.getProperty(
					"/properties/message/"
							+ ConnectorProperties.getInstance().getProperty(
									"/properties/database/ack/message/file"));

	public static String TABLE_NAME = ConnectorProperties.getInstance()
			.getProperty("/properties/database/table/name");

	protected static String tableStatus = ConnectorProperties.getInstance()
			.getProperty("/properties/database/table/status");

	protected static String notSentClause = tableStatus
			+ "="
			+ ConnectorProperties.getInstance().getProperty(
					"/properties/database/table/not_sent");

	protected static String sentClause = tableStatus
			+ "="
			+ ConnectorProperties.getInstance().getProperty(
					"/properties/database/table/sent");

	String msgType = ConnectorProperties.getInstance().getProperty(
			"/properties/database/message/type");

	String msgProfile = ConnectorProperties.getInstance().getProperty(
			"/properties/database/message/profile");

	boolean validationsEnabled = Boolean.parseBoolean(ConnectorProperties
			.getInstance().getProperty(
					"/properties/database/message/validation").trim());

	String ackType = ConnectorProperties.getInstance().getProperty(
			"/properties/database/ack/message/type");

	String ackProfile = ConnectorProperties.getInstance().getProperty(
			"/properties/database/ack/message/profile");

	String msaIdField = ConnectorProperties.getInstance().getProperty(
			"/properties/database/ack/msgId");

	String mshIdField = ConnectorProperties.getInstance().getProperty(
			"/properties/database/message/id");

	String messageIdColumn = ConnectorProperties.getInstance().getProperty(
			"/properties/database/table/id");

	String returnedCodeColumn = ConnectorProperties.getInstance().getProperty(
			"/properties/database/table/code");

	String errorMessageColumn = ConnectorProperties.getInstance().getProperty(
			"/properties/database/table/message");

	static final String PREFIX_ACK = "/properties/database/ack/";

	String ackCode = ConnectorProperties.getInstance().getProperty(
			PREFIX_ACK + "code");

	String ackErrorMessage = ConnectorProperties.getInstance().getProperty(
			PREFIX_ACK + "error");

	/**
	 * HL7 code indicating that message will be resent
	 */
	String resendCode = ConnectorProperties.getInstance().getProperty(
			PREFIX_ACK + "resend_code");

	boolean testMode = Boolean.parseBoolean(ConnectorProperties.getInstance()
			.getProperty(PREFIX_ACK + "testing").trim());

	/**
	 * Table column to field map value(column name) key(field path)
	 */
	String COL_PATH = "/properties/database/map/column";

	HashMap<String, Column> tableFieldMap = ConnectorProperties.getInstance()
			.getColumnToFieldMaps(COL_PATH);

	/**
	 * Fixed fields for the
	 */
	HashMap<String, Column> fixedFieldMap = ConnectorProperties.getInstance()
			.getColumnToFieldMaps("/properties/database/map/field");

	String findUnsentMessagesStmt = "Select * from " + TABLE_NAME + " where "
			+ notSentClause;

	/**
	 * Interval between polling
	 */
	long pollingInterval = Long.parseLong(ConnectorProperties.getInstance()
			.getProperty("/properties/database/polling/interval").trim());

	/**
	 * Default Constructor
	 * 
	 * @param msgDefFile
	 * @param ackDefFile
	 */
	public DatabasePoller(String msgDefFile, String ackDefFile) {

		super(msgDefFile, ackDefFile);

		if (this.isValidationsEnabled()) {
			logger.info("M7Message Profile Validation is enabled.");
		}
		if (this.isTestMode()) {
			logger.warn("TEST MODE - continuous re-send of all messsages in '"
					+ TABLE_NAME + "'");
		}
		// register the shutdown hook
		Runtime.getRuntime().addShutdownHook(new Thread() {
			public void run() {
				clearConnection();
			}
		});
	}

	public DatabasePoller() {
		this(MSG_DEF_FILE_NAME, ACK_DEF_FILE_NAME);
	}

	/**
	 * Poll the database and retrieve the message objects
	 * 
	 * @return message vector
	 */
	public Vector<M7Message> poll() {
		Statement stmt = null;
		ResultSet rs = null;
		Vector<M7Message> recordSetVector = null;
		/**
		 * Vector of key(table column) to value (column value) maps
		 */
		Vector<HashMap> rowMapVector = new Vector<HashMap>();
		this.refreshConnection();
		try {
			stmt = dbConnection.createStatement();
					//ResultSet.TYPE_SCROLL_INSENSITIVE,
					//ResultSet.CONCUR_UPDATABLE);

			rs = stmt.executeQuery(findUnsentMessagesStmt);

			while (rs.next()) {
				rowMapVector.add(readRow(rs, tableFieldMap));
			}

			recordSetVector = createMessageVector(rowMapVector,
					this.tableFieldMap, this.fixedFieldMap);

			if (rs != null) {
				rs.close();
			}

		} catch (SQLException ex) {
			ex.printStackTrace();
			logger.error("SQLException: " + ex.getMessage());
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());
			// logger.error("Reconnecting due to SQLException: " +
			// e.getMessage());
			// clearConnection();
			// connect();
		}
		return recordSetVector;
	}

	/**
	 * Update the staging table using the response message content
	 * 
	 * @param ackMsgStr
	 */
	public void updateResponse(String ackMsgStr) {
		M7Message msg = null;
		try {
			msg = new M7Message(ackMsgStr, adf);

		} catch (M7Exception e1) {
			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		}
		if (msg != null) {
			try {
				String code = msg.getFieldValue(ackCode);
				String id = msg.getFieldValue(msaIdField);
				String error = "";
				if (msg.getFieldState(ackErrorMessage) == M7Field.eFldPresent) {
					error = msg.getFieldValue(ackErrorMessage);
				}
				updateRow(id, code, error);

			} catch (M7Exception e) {
				logger.error("Missing fields in the response:" + ackMsgStr
						+ '\n' + e.getLocalizedMessage());
			}
		}
	}

	/**
	 * Creates a vector of messages from the vector of row-value hash maps
	 * 
	 * @param rowVector
	 *            contains the columns and their values
	 * @return Vector of message objects (M7Message)
	 */
	protected Vector<M7Message> createMessageVector(
			Vector<HashMap> rowVector, HashMap<String, Column> fieldColumMap,
			HashMap<String, Column> fixedMap) {

		Vector<M7Message> messageVector = new Vector<M7Message>();
		// iterate the mappings
		for (int i = 0; i < rowVector.size(); i++) {
			HashMap<String, ?> rowMap = rowVector.elementAt(i);
			M7Message msg = msgFromRow(rowMap, fieldColumMap, fixedMap);

			if (this.isValidationsEnabled()) {
				StringBuffer errors = new StringBuffer(
						"  Validation failed:  \n________________\n");
				msg.setMessageProfileId(this.msgProfile);
				boolean isValid = false;
				try {
					isValid = msg.validate(errors);
				} catch (M7Exception e) {
					logger.error(e.getLocalizedMessage());
				}
				if (isValid) {
					messageVector.add(msg);
				} else {
					try {
						String id = msg.getFieldValue(this.getMessageIdField());
						updateRow(id, "AE", errors.toString());
					} catch (M7Exception e) {
						e.printStackTrace();
						logger.error("Missing " + this.getMessageIdField()
								+ "; " + e.getLocalizedMessage());
					}
				}
			} else {
				messageVector.add(msg);
			}
		}
		return messageVector;
	}

	/**
	 * Creates a message instance from a row's hash map
	 * 
	 * @param rowMap
	 *            HashMap
	 * @param fieldMap
	 *            HashMap
	 * @param fixedFields
	 *            HashMap
	 * @return message M7Message
	 */
	protected M7Message msgFromRow(HashMap<String, ?> rowMap, HashMap<String, Column> fieldMap,
			HashMap<String, Column> fixedFields) {

		M7Message msg = null;
		try {
			msg = new M7Message(mdf, msgType);
		} catch (M7Exception e1) {
			e1.printStackTrace();
			logger.error(e1.getLocalizedMessage());
		}
		Iterator<String> iterator = rowMap.keySet().iterator();
		while (iterator.hasNext()) {
			String column = iterator.next();
			String dbValue = (String) rowMap.get(column);
			// since one column may be used many times, find each instance
			Iterator<String> fieldIterator = fieldMap.keySet().iterator();
			while (fieldIterator.hasNext()) {
				String field = fieldIterator.next();
				Column col = fieldMap.get(field);
				if (column.equalsIgnoreCase(col.getColumnName())) {
					logger.debug(field + " -> " + dbValue + " -> " + column);
					if (dbValue != null) {
						// remove trailing spaces
						dbValue = dbValue.trim();
						// check the multiplicity and length

						// check the value in the db against the regex to see if
						// it's an timestamp
						try {
							if (dbValue.matches(M7Date.regExSQL)) {
								msg.setFieldValue(field, new M7Date(dbValue));
							} else if (dbValue.matches(M7DateTime.regExSQL)) {
								msg.setFieldValue(field,
										new M7DateTime(dbValue));
							} else {
								msg.setFieldValue(field, escapeChars(dbValue));
							}
						} catch (M7Exception e) {

							e.printStackTrace();
							logger.error(e.getLocalizedMessage());

						}
					}
				}
			}
		}
		iterator = fixedFields.keySet().iterator();
		while (iterator.hasNext()) {
			String field = iterator.next();
			Column value = fixedFields.get(field);
			if (value == null) {
				logger.warn("Null value for '" + field + "'");
			} else {
				try {
					if (value.getValue().contains(new StringBuffer("$"))) {
						if (value.getValue().equalsIgnoreCase("$datetime")) {
							logger.debug("Insert current date/time in field: "+field);
							msg.setFieldValue(field, new M7DateTime());
						}
					} else {
						msg.setFieldValue(field, value.getValue());
					}
				} catch (M7Exception ex) {
					ex.printStackTrace();
					logger.error(ex);
				}
			}
		}
		iterator = rowMap.keySet().iterator();
		while (iterator.hasNext()) {
			String column = iterator.next();
			String dbValue = (String) rowMap.get(column);
			// since one column may be used many times, find each instance
			Iterator<String> fieldIterator = fieldMap.keySet().iterator();
			while (fieldIterator.hasNext()) {
				String field = fieldIterator.next();
				Column col = fieldMap.get(field);
				try {
					if ((dbValue != null) && (col.getMultiplicity() > 0)
							&& (dbValue.length() > col.getMaxLength())) {
						dbValue = escapeChars(dbValue).trim();
						msg.setFieldValue(field, escapeChars(dbValue.substring(
								0, (col.getMaxLength() - 1))));
						String segName = field.substring(0, (field
								.indexOf(M7Message.ABS_NAME_DELM) - 1));
						int index = segName.indexOf("[");
						if (index > 0) {
							segName = segName.substring(0, index);
						}
						String fieldPath = field.substring(field
								.indexOf(M7Message.ABS_NAME_DELM) + 1, field
								.length());
						M7Repeat segment = msg.getRepeat(segName);
						M7Composite obx = msg.getChild(segName + "[0]");
						int lines = dbValue.length() / col.getMaxLength();
						for (int i = 1; i < (lines + 1); i++) {
							M7Composite duplicate = segment.add();
							// segment.copy(duplicate);
							int lowLimit = i * col.getMaxLength() - 1;
							if (lowLimit <= dbValue.length()) {
								int highLimit = ((i + 1) * col.getMaxLength()) - 1;
								if (highLimit >= dbValue.length()) {
									highLimit = dbValue.length();
								}
								/*
								 * if (dbValue.length() >= (((i + 1) * col
								 * .getMaxLength()) - 1)) { index = (((i + 1) *
								 * col.getMaxLength()) - 1); } else { index =
								 * dbValue.length()-1; }
								 */
								duplicate.setFieldValue("SetIDOBX", i + 1);
								duplicate.setFieldValue("ValueType", obx
										.getFieldValue("ValueType"));
								duplicate
										.setFieldValue(
												"ObservationIdentifier.identifier",
												obx
														.getFieldValue("ObservationIdentifier.identifier"));

								duplicate
										.setFieldValue(
												"ObservationIdentifier.text",
												obx
														.getFieldValue("ObservationIdentifier.text"));

								duplicate.setFieldValue(fieldPath, dbValue
										.substring(lowLimit, highLimit));
							}
						}
					}
				} catch (M7Exception e) {
					e.printStackTrace();
					logger.error(e.getLocalizedMessage());
				}
			}
		}
		return msg;
	}

	/**
	 * Update the staging table row using the contents of the ack
	 * 
	 * @param id
	 * @param code
	 * @param error
	 */
	public synchronized void updateRowError(String id, String code, String error) {
		Statement stmt = null;
		String stmtStr = null;
		int records = 0;
		try {
			stmt = dbConnection.createStatement();
					//ResultSet.TYPE_SCROLL_INSENSITIVE,
					//ResultSet.CONCUR_UPDATABLE);

		} catch (SQLException ex) {
			logger.error("SQLException: " + ex.getMessage());
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());
			System.exit(0);

		}
		try {
			if (!testMode) {
				if (!code.equalsIgnoreCase(resendCode)) {
					stmtStr = "UPDATE "
							+ TABLE_NAME
							+ " SET "
							+ errorMessageColumn
							+ "='"
							+ error
							+ "',"
							+ returnedCodeColumn
							+ "='"
							+ ConnectorProperties.getInstance().getProperty(
									PREFIX_ACK + code) + "', " + sentClause
							+ " WHERE " + messageIdColumn + "=" + id;
					logger.debug("Updating row using " + stmtStr);
				} else // use 'notSentClause' so we re-send it next time
				{
					stmtStr = "UPDATE "
							+ TABLE_NAME
							+ " SET "
							+ errorMessageColumn
							+ "='"
							+ error
							+ "',"
							+ returnedCodeColumn
							+ "='"
							+ ConnectorProperties.getInstance().getProperty(
									PREFIX_ACK + code) + "', " + notSentClause
							+ " WHERE " + messageIdColumn + "=" + id;

				}
				records = stmt.executeUpdate(stmtStr);
			}

		} catch (SQLException ex) {

			logger.error("SQLException: " + ex.getMessage());
			logger.error("SQLState: " + ex.getSQLState());
			logger.error("VendorError: " + ex.getErrorCode());
			System.exit(0);
		}
		try {
			stmt.close();
		} catch (SQLException e) {
			logger.error(e.getLocalizedMessage());
			e.printStackTrace();
		}
		if (records == 0) {
			logger.info("The response message control id:" + id
					+ " does not match an initiating " + this.msgType
					+ " message");
		}

	}

	public String getMessageIdField() {
		return mshIdField;
	}

	public boolean isValidationsEnabled() {
		return validationsEnabled;
	}

	public boolean isTestMode() {
		return testMode;
	}

	public String getResendCode() {
		return resendCode;
	}

	@Override
	public String getColumnPath() {
		return COL_PATH;
	}


	private String escapeChars(String value) {
		return value.replace('\n', ' ').replace('|', '-').replace('&', '+').replace('\\',' ').replace('^', ' ');

	}
}
