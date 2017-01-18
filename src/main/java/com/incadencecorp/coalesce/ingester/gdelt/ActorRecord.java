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

	public static CoalesceRecordset createRecordSet(CoalesceSection section, String pathName) {
		CoalesceRecordset recordSet = CoalesceRecordset.create(section, pathName);
		
		CoalesceFieldDefinition.create(recordSet, "Actor1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Name", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1KnownGroupCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1EthnicCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Religion1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Religion2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Type1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Type2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Type3Code", ECoalesceFieldDataTypes.STRING_TYPE);
		
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_Type", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_Fullname", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_CountryCode", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_ADM1Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_ADM2Code", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_Lat", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_Long", ECoalesceFieldDataTypes.FLOAT_TYPE);
		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_FeatureID", ECoalesceFieldDataTypes.STRING_TYPE);

		CoalesceFieldDefinition.create(recordSet, "Actor1Geo_Location",
				ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);
		
		return recordSet;
	}

}
