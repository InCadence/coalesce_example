package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class OEArtifact extends OEEntity {

	public OEArtifact() {
		
		CoalesceSection artifactSection = CoalesceSection.create(this, "ArtifactSection");
		CoalesceRecordset artifactRecordset = CoalesceRecordset.create(artifactSection, "ArtifactRecordset");
		
		artifactRecordset.addNew();
		
		CoalesceFieldDefinition.create(artifactRecordset, "Md5Sum", ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(artifactRecordset, "DateIngested", ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		CoalesceFieldDefinition.create(artifactRecordset, "ArtifactDate", ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		
	}

}
