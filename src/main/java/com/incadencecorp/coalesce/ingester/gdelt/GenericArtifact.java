package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GenericArtifact extends GDELTEntity {

	public GenericArtifact() {
		initialize();
		
		CoalesceSection artifactSection = CoalesceSection.create(this, ArtifactConstants.ArtifactSection);
		CoalesceRecordset artifactRecordset = CoalesceRecordset.create(artifactSection, ArtifactConstants.ArtifactRecordset);
		
		CoalesceFieldDefinition.create(artifactRecordset, ArtifactConstants.Md5Sum, ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(artifactRecordset, ArtifactConstants.DateIngested, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		CoalesceFieldDefinition.create(artifactRecordset, ArtifactConstants.ArtifactDate, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		
		artifactRecordset.addNew();
	}
	public  static String getRecordSetName() {
		return ArtifactConstants.ArtifactRecordset;
	}

	public static  String getQueryName() {
		return ArtifactConstants.ArtifactRecordset;
	}
}
