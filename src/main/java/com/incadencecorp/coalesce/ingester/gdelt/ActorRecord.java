package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class ActorRecord extends OERecord {

	public ActorRecord() {

	}

	public ActorRecord(CoalesceRecord record) {
		super(record);

	}

	public static CoalesceRecordset createRecordset(CoalesceSection section, String pathName) {
		CoalesceRecordset recordSet = CoalesceRecordset.create(section, pathName);
		
		CoalesceFieldDefinition.create(recordSet, "ActorCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorName", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "NameMetaphone", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorCountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorKnownGroupCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorEthnicCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorReligion1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorReligion2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorType1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorType2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorType3Code", ECoalesceFieldDataTypes.STRING_TYPE);
		
		CoalesceFieldDefinition.create(recordSet, "ActorGeoType", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorGeoFullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorGeoCountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorGeoADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorGeoADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "ActorGeoFeatureID", ECoalesceFieldDataTypes.STRING_TYPE);
		
		CoalesceFieldDefinition.create(recordSet, "ActorGeoLocation",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		
		return recordSet;
	}

}
