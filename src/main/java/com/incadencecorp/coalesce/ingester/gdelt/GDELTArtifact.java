package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELTArtifact extends OEArtifact {
	//protected static String VERSION = "0.1";
	private static String TITLE = "GDELTArtifact";
	public GDELTArtifact() {
		
		setName("GDELTArtifact");
		setSource("GDELT");
		setVersion(VERSION);
		setTitle(TITLE);

		
		CoalesceSection gdeltArtifactSection = CoalesceSection.create(this, "GDELTArtifactSection");
		CoalesceRecordset gdeltArtifactRecordset = CoalesceRecordset.create(gdeltArtifactSection, "GDELTArtifactRecordset");
		
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, "SourceFileName", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, "GlobalEventID", ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, "RawText", ECoalesceFieldDataTypes.STRING_TYPE);
		
		gdeltArtifactRecordset.addNew();
	}

}
