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
		
		CoalesceFieldDefinition.create(recordSet, "DataSource", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "OntologyReference", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "IsSimulation", ECoalesceFieldDataTypes.BOOLEAN_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTPolitical", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTMilitary", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTEconomic", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTSocial", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTInformation", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTInfrastructure", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTPhysicalEnvironment", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "PMSIIPTTime", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Tags", ECoalesceFieldDataTypes.STRING_LIST_TYPE);
		
		return recordSet;
	}
}
