/**
 * 
 */
package com.incadencecorp.coalesce.ingester.gdelt;

import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;

public class GDELTRecord extends CoalesceRecord {

    private CoalesceRecordset recordSet;

    private String DataSource;
    private String OntologyReference;
    private String IsSimulation;
    private String PMESIIPTPolitical;
    private String PMESIIPTMilitary;
    private String PMESIIPTEconomic;
    private String PMESIIPTSocial;
    private String PMESIIPTInformation;
    private String PMESIIPTInfrastructure;
    private String PMESIIPTPhysicalEnvironment;
    private String PMESIIPTTime;
    private String Tags;


    /**
     * @return the recordSet
     */
    public CoalesceRecordset getRecordSet()
    {
        return recordSet;
    }

    /**
     * @param recordSet the recordSet to set
     */
    public void setRecordSet(CoalesceRecordset recordSet)
    {
        this.recordSet = recordSet;
    }

    /**
     * @return the dataSource
     */
    public String getDataSource()
    {
        return DataSource;
    }

    /**
     * @param dataSource the dataSource to set
     */
    public void setDataSource(String dataSource)
    {
        DataSource = dataSource;
    }

    /**
     * @return the ontologyReference
     */
    public String getOntologyReference()
    {
        return OntologyReference;
    }

    /**
     * @param ontologyReference the ontologyReference to set
     */
    public void setOntologyReference(String ontologyReference)
    {
        OntologyReference = ontologyReference;
    }

    /**
     * @return the isSimulation
     */
    public String getIsSimulation()
    {
        return IsSimulation;
    }

    /**
     * @param isSimulation the isSimulation to set
     */
    public void setIsSimulation(String isSimulation)
    {
        IsSimulation = isSimulation;
    }

    /**
     * @return the pMESIIPTPolitical
     */
    public String getPMESIIPTPolitical()
    {
        return PMESIIPTPolitical;
    }

    /**
     * @param pMESIIPTPolitical the pMESIIPTPolitical to set
     */
    public void setPMESIIPTPolitical(String pMESIIPTPolitical)
    {
        PMESIIPTPolitical = pMESIIPTPolitical;
    }

    /**
     * @return the pMESIIPTMilitary
     */
    public String getPMESIIPTMilitary()
    {
        return PMESIIPTMilitary;
    }

    /**
     * @param pMESIIPTMilitary the pMESIIPTMilitary to set
     */
    public void setPMESIIPTMilitary(String pMESIIPTMilitary)
    {
        PMESIIPTMilitary = pMESIIPTMilitary;
    }

    /**
     * @return the pMESIIPTEconomic
     */
    public String getPMESIIPTEconomic()
    {
        return PMESIIPTEconomic;
    }

    /**
     * @param pMESIIPTEconomic the pMESIIPTEconomic to set
     */
    public void setPMESIIPTEconomic(String pMESIIPTEconomic)
    {
        PMESIIPTEconomic = pMESIIPTEconomic;
    }

    /**
     * @return the pMESIIPTSocial
     */
    public String getPMESIIPTSocial()
    {
        return PMESIIPTSocial;
    }

    /**
     * @param pMESIIPTSocial the pMESIIPTSocial to set
     */
    public void setPMESIIPTSocial(String pMESIIPTSocial)
    {
        PMESIIPTSocial = pMESIIPTSocial;
    }

    /**
     * @return the pMESIIPTInformation
     */
    public String getPMESIIPTInformation()
    {
        return PMESIIPTInformation;
    }

    /**
     * @param pMESIIPTInformation the pMESIIPTInformation to set
     */
    public void setPMESIIPTInformation(String pMESIIPTInformation)
    {
        PMESIIPTInformation = pMESIIPTInformation;
    }

    /**
     * @return the pMESIIPTInfrastructure
     */
    public String getPMESIIPTInfrastructure()
    {
        return PMESIIPTInfrastructure;
    }

    /**
     * @param pMESIIPTInfrastructure the pMESIIPTInfrastructure to set
     */
    public void setPMESIIPTInfrastructure(String pMESIIPTInfrastructure)
    {
        PMESIIPTInfrastructure = pMESIIPTInfrastructure;
    }

    /**
     * @return the pMESIIPTPhysicalEnvironment
     */
    public String getPMESIIPTPhysicalEnvironment()
    {
        return PMESIIPTPhysicalEnvironment;
    }

    /**
     * @param pMESIIPTPhysicalEnvironment the pMESIIPTPhysicalEnvironment to set
     */
    public void setPMESIIPTPhysicalEnvironment(String pMESIIPTPhysicalEnvironment)
    {
        PMESIIPTPhysicalEnvironment = pMESIIPTPhysicalEnvironment;
    }

    /**
     * @return the pMESIIPTTime
     */
    public String getPMESIIPTTime()
    {
        return PMESIIPTTime;
    }

    /**
     * @param pMESIIPTTime the pMESIIPTTime to set
     */
    public void setPMESIIPTTime(String pMESIIPTTime)
    {
        PMESIIPTTime = pMESIIPTTime;
    }

    /**
     * @return the tags
     */
    public String getTags()
    {
        return Tags;
    }

    /**
     * @param tags the tags to set
     */
    public void setTags(String tags)
    {
        Tags = tags;
    }

    /**
     * 
     */
    public GDELTRecord()
    {

    }

    /**
     * @param record
     */
    public GDELTRecord(CoalesceRecord record)
    {
        super(record);
    }

    public CoalesceRecordset createRecordSet(CoalesceSection section, String pathName)
    {
        recordSet = CoalesceRecordset.create(section, pathName);

        CoalesceFieldDefinition.create(recordSet, GDELTEntityConstants.DataSource, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTEntityConstants.OntologyReference,
                                       ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEntityConstants.IsSimulation, ECoalesceFieldDataTypes.BOOLEAN_TYPE);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTEntityConstants.PMESIIPTPolitical,
                                       ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEntityConstants.PMESIIPTMilitary, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEntityConstants.PMESIIPTEconomic, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEntityConstants.PMESIIPTSocial, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTEntityConstants.PMESIIPTInformation,
                                       ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTEntityConstants.PMESIIPTInfrastructure,
                                       ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet,
                                       GDELTEntityConstants.PMESIIPTPhysicalEnvironment,
                                       ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEntityConstants.PMESIIPTTime, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTEntityConstants.Tags, ECoalesceFieldDataTypes.STRING_LIST_TYPE);

        // event record setup
        CoalesceFieldDefinition.create(recordSet, EventConstants.GlobalEventID, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.Day, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.MonthYear, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.Year, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.FractionDate, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(recordSet, EventConstants.IsRootEvent, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.EventCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.EventBaseCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.EventRootCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.QuadClass, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.GoldsteinScale, ECoalesceFieldDataTypes.FLOAT_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.NumMentions, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.NumSources, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.NumArticles, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.AvgTone, ECoalesceFieldDataTypes.FLOAT_TYPE);

        CoalesceFieldDefinition.create(recordSet, EventConstants.ActionGeoType, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.ActionGeoFullname, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.ActionGeoCountryCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.ActionGeoADM1Code, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.ActionGeoADM2Code, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.ActionGeoFeatureID, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.DateAdded, ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.SourceURL, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, EventConstants.DateTime, ECoalesceFieldDataTypes.DATE_TIME_TYPE);

        CoalesceFieldDefinition.create(recordSet,
                                       EventConstants.ActionGeoLocation,
                                       ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        // agent record setup
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentCode, ECoalesceFieldDataTypes.STRING_TYPE, false);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentName, ECoalesceFieldDataTypes.STRING_TYPE, false);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.NameMetaphone, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       false);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentKnownGroupCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentEthnicCode, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentReligion1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentReligion2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentType1Code, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentType2Code, ECoalesceFieldDataTypes.STRING_TYPE, true);
        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentType3Code, ECoalesceFieldDataTypes.STRING_TYPE, true);

        CoalesceFieldDefinition.create(recordSet, AgentConstants.AgentGeoType, ECoalesceFieldDataTypes.INTEGER_TYPE, true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoFullname,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoCountryCode,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoADM1Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoADM2Code,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);
        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoFeatureID,
                                       ECoalesceFieldDataTypes.STRING_TYPE,
                                       true);

        CoalesceFieldDefinition.create(recordSet,
                                       AgentConstants.AgentGeoLocation,
                                       ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        return recordSet;
    }
}
