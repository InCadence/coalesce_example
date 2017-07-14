package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GenericArtifact extends GDELTEntity {

	public GenericArtifact() {
		initialize();
		
		CoalesceSection artifactSection = CoalesceSection.create(this, GDELTArtifactConstants.ArtifactSection);
		CoalesceRecordset artifactRecordset = CoalesceRecordset.create(artifactSection, GDELTArtifactConstants.ArtifactRecordset);
		
		CoalesceFieldDefinition.create(artifactRecordset, GDELTArtifactConstants.Md5Sum, ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(artifactRecordset, GDELTArtifactConstants.DateIngested, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		CoalesceFieldDefinition.create(artifactRecordset, GDELTArtifactConstants.ArtifactDate, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		
		artifactRecordset.addNew();
	}
	public  static String getRecordSetName() {
		return GDELTArtifactConstants.ArtifactRecordset;
	}

	public static  String getQueryName() {
		return GDELTArtifactConstants.ArtifactRecordset;
	}
}
