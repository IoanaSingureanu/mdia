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
package org.iexhub.connectors.database;

/**
 * Class Column: Contains the column name, value, maxlength, multiplicity, etc.
 * 
 * @author Ioana Singureanu
 * @version 1.0
 * 
 */
public class Column {

	String columnName = null;
	
	String fieldName = null;

	String value = null;

	int maxLength = -1;

	int multiplicity = -1;
	
	boolean isPrimaryKey = false;
	
	boolean isUpdateable = false;



	public Column() {
		super();
	}

	public Column(String columnName, String value) {
		super();
		this.columnName = columnName;
		this.value = value;
	}

	public Column(String columnName, String value, int maxLength,
			int multiplicity) {
		super();
		this.columnName = columnName;
		this.value = value;
		this.maxLength = maxLength;
		this.multiplicity = multiplicity;
	}

	public Column(String columnName, int maxLength, int multiplicity, 
			String fieldName, String value, boolean isPrimaryKey, boolean isUpdateable ) {
		super();
		this.columnName = columnName;
		this.maxLength = maxLength;
		this.multiplicity = multiplicity;
		this.fieldName= fieldName;
		this.value = value;
		this.isPrimaryKey = isPrimaryKey;
		this.isUpdateable = isUpdateable;
	}
	
	public Column(Column original)
	{
		this(original.getColumnName(), original.getMaxLength(), original.getMultiplicity(),
				original.getFieldName(), original.getValue(), original.isPrimaryKey, original.isUpdateable);
	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public int getMaxLength() {
		return maxLength;
	}

	public void setMaxLength(int maxLength) {
		this.maxLength = maxLength;
	}

	@Override
	protected Object clone() throws CloneNotSupportedException {
		
		 super.clone();
		 return new Column();
	}

	public int getMultiplicity() {
		return multiplicity;
	}

	public void setMultiplicity(int multiplicity) {
		this.multiplicity = multiplicity;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}
	
	public boolean isPrimaryKey() {
		return isPrimaryKey;
	}

	public void setPrimaryKey(boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}
	public String getFieldName() {
		return fieldName;
	}

	public void setFieldName(String fieldName) {
		this.fieldName = fieldName;
	}
	public String toString()
	{
		return columnName +" = " + value+"; pk ="+ this.isPrimaryKey()+"; is updateable = "+this.isUpdateable();
	}

	public boolean isUpdateable() {
		return isUpdateable;
	}

	public void setUpdateable(boolean isUpdateable) {
		this.isUpdateable = isUpdateable;
	}


}
