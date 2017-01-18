/**
 * 
 */
package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

/**
 * @author ckrentz
 *
 */
public class OERecord extends CoalesceRecord {

	/**
	 * 
	 */
	public OERecord() {

	}

	/**
	 * @param record
	 */
	public OERecord(CoalesceRecord record) {
		super(record);

	}
	
	public static CoalesceRecordset createRecordSet(CoalesceSection section, String pathName) {
		CoalesceRecordset recordSet = CoalesceRecordset.create(section, pathName);
		
		CoalesceFieldDefinition.create(recordSet, "source", ECoalesceFieldDataTypes.STRING_TYPE);
		
		return recordSet;
	}
}
