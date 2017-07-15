/**
 * 
 */
package com.incadencecorp.coalesce.ingester.gdelt;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.incadencecorp.coalesce.common.exceptions.CoalesceDataFormatException;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceIntegerField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceStringField;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;





/**
 * @author BigDataDaddy
 *
 */
public class GDELTArtifactRecord extends CoalesceRecord {
	private final static Logger LOGGER = LoggerFactory.getLogger(GDELTEventRecord.class);	
	
	public GDELTArtifactRecord() {

	}

	public GDELTArtifactRecord(CoalesceRecord record) {
		super(record);

	}
	
	public String getSourceFileName() {
		return ((CoalesceStringField) this.getFieldByName(GDELTArtifactConstants.SourceFileName)).getValue();
	}
	public void setSourceFileName(String sourceFileName) {
		 ((CoalesceStringField) this.getFieldByName(GDELTArtifactConstants.SourceFileName)).setValue(sourceFileName);
	}
	public int getGlobalEventID()  {
		try {
			return ((CoalesceIntegerField) this.getFieldByName(GDELTArtifactConstants.GlobalEventID)).getValue();
		} catch (CoalesceDataFormatException e) {
			LOGGER.error(e.getMessage(),e);
			return 0;
		}
	}

	public void setGlobalEventID(int globalEventID) {
		 ((CoalesceIntegerField) this.getFieldByName(GDELTArtifactConstants.GlobalEventID)).setValue(globalEventID);
	}

	public String getRawText() {
		return ((CoalesceStringField) this.getFieldByName(GDELTArtifactConstants.RawText)).getValue();
	}
	public void setRawText(String rawText) {
		((CoalesceStringField) this.getFieldByName(GDELTArtifactConstants.RawText)).setValue(rawText);
	}
	
	public static CoalesceRecordset createRecordSet(CoalesceSection section, String pathName) {
		CoalesceRecordset gdeltArtifactRecordset = CoalesceRecordset.create(section, pathName);
		
		
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTArtifactConstants.Md5Sum, ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTArtifactConstants.DateIngested, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTArtifactConstants.ArtifactDate, ECoalesceFieldDataTypes.DATE_TIME_TYPE);
		
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTArtifactConstants.SourceFileName, ECoalesceFieldDataTypes.STRING_TYPE);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTArtifactConstants.GlobalEventID, ECoalesceFieldDataTypes.INTEGER_TYPE);
		CoalesceFieldDefinition.create(gdeltArtifactRecordset, GDELTArtifactConstants.RawText, ECoalesceFieldDataTypes.STRING_TYPE);
		
		return gdeltArtifactRecordset;
	}

}
