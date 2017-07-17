package com.incadencecorp.coalesce.ingester.gdelt;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.incadencecorp.coalesce.common.exceptions.CoalesceDataFormatException;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceCoordinateField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceFieldDefinition;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceIntegerField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceSection;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceStringField;
import com.incadencecorp.coalesce.framework.datamodel.ECoalesceFieldDataTypes;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.GeometryFactory;
import com.vividsolutions.jts.geom.Point;

public class GDELTAgentRecord extends CoalesceRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(GDELTAgentRecord.class);

    private static GeometryFactory factory = new GeometryFactory();

    public GDELTAgentRecord()
    {

    }

    /**
     * @param record
     */
    public GDELTAgentRecord(CoalesceRecord record)
    {
        super(record);
    }

    /**
     * @return the agentCode
     */
    public String getAgentCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentCode)).getValue();
    }

    /**
     * @param agentCode the agentCode to set
     */
    public void setAgentCode(String agentCode)
    {
        if (StringUtils.isNotBlank(agentCode))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentCode)).setValue(agentCode);
        }
    }

    /**
     * @return the agentName
     */
    public String getAgentName()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentName)).getValue();
    }

    /**
     * @param agentName the agentName to set
     */
    public void setAgentName(String agentName)
    {
        if (StringUtils.isNotBlank(agentName))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentName)).setValue(agentName);
        }
    }

    /**
     * @return the NameMetaphone
     */
    public String getNameMetaphone()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.NameMetaphone)).getValue();
    }

    /**
     * @param NameMetaphone the NameMetaphone to set
     */
    public void setNameMetaphone(String NameMetaphone)
    {
        if (StringUtils.isNotBlank(NameMetaphone))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.NameMetaphone)).setValue(NameMetaphone);
        }
    }

    /**
     * @return the agentCountryCode
     */
    public String getAgentCountryCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentCountryCode)).getValue();
    }

    /**
     * @param AgentCountryCode the AgentCountryCode to set
     */
    public void setAgentCountryCode(String AgentCountryCode)
    {
        if (StringUtils.isNotBlank(AgentCountryCode))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentCountryCode)).setValue(AgentCountryCode);
        }
    }

    /**
     * @return the agentKnownGroupCode
     */
    public String getAgentKnownGroupCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentKnownGroupCode)).getValue();
    }

    /**
     * @param AgentKnownGroupCode the AgentKnownGroupCode to set
     */
    public void setAgentKnownGroupCode(String AgentKnownGroupCode)
    {
        if (StringUtils.isNotBlank(AgentKnownGroupCode))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentKnownGroupCode)).setValue(AgentKnownGroupCode);
        }
    }

    /**
     * @return the agentEthnicCode
     */
    public String getAgentEthnicCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentEthnicCode)).getValue();
    }

    /**
     * @param AgentEthnicCode the AgentEthnicCode to set
     */
    public void setAgentEthnicCode(String AgentEthnicCode)
    {
        if (StringUtils.isNotBlank(AgentEthnicCode))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentEthnicCode)).setValue(AgentEthnicCode);
        }
    }

    /**
     * @return the agentReligion1Code
     */
    public String getAgentReligion1Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentReligion1Code)).getValue();
    }

    /**
     * @param AgentReligion1Code the AgentReligion1Code to set
     */
    public void setAgentReligion1Code(String AgentReligion1Code)
    {
        if (StringUtils.isNotBlank(AgentReligion1Code))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentReligion1Code)).setValue(AgentReligion1Code);
        }
    }

    /**
     * @return the agentReligion2Code
     */
    public String getAgentReligion2Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentReligion2Code)).getValue();
    }

    /**
     * @param AgentReligion2Code the AgentReligion2Code to set
     */
    public void setAgentReligion2Code(String AgentReligion2Code)
    {
        if (StringUtils.isNotBlank(AgentReligion2Code))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentReligion2Code)).setValue(AgentReligion2Code);
        }
    }

    /**
     * @return the agentType1Code
     */
    public String getAgentType1Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentType1Code)).getValue();
    }

    /**
     * @param AgentType1Code the AgentType1Code to set
     */
    public void setAgentType1Code(String AgentType1Code)
    {
        if (StringUtils.isNotBlank(AgentType1Code))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentType1Code)).setValue(AgentType1Code);
        }
    }

    /**
     * @return the agentType2Code
     */
    public String getAgentType2Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentType2Code)).getValue();
    }

    /**
     * @param AgentType2Code the AgentType2Code to set
     */
    public void setAgentType2Code(String AgentType2Code)
    {
        if (StringUtils.isNotBlank(AgentType2Code))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentType2Code)).setValue(AgentType2Code);
        }
    }

    /**
     * @return the agentType3Code
     */
    public String getAgentType3Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentType3Code)).getValue();
    }

    /**
     * @param AgentType3Code the AgentType3Code to set
     */
    public void setAgentType3Code(String AgentType3Code)
    {
        if (StringUtils.isNotBlank(AgentType3Code))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentType3Code)).setValue(AgentType3Code);
        }
    }

    /**
     * @return the agentGeoType
     */
    public Integer getAgentGeoType()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(GDELTAgentConstants.AgentGeoType)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            // TODO Auto-generated catch block
            LOGGER.error(e.getMessage(), e);
            return 0;
        }
    }

    /**
     * @param AgentGeoType the AgentGeoType to set
     */
    public void setAgentGeoType(String AgentGeoType)
    {
        if (StringUtils.isNotBlank(AgentGeoType) && NumberUtils.isNumber(AgentGeoType))
        {
            ((CoalesceIntegerField) this.getFieldByName(GDELTAgentConstants.AgentGeoType)).setValue(NumberUtils.toInt(AgentGeoType));
        }
    }

    /**
     * @param AgentGeoType the AgentGeoType to set
     */
    public void setAgentGeoType(int AgentGeoType)
    {
        ((CoalesceIntegerField) this.getFieldByName(GDELTAgentConstants.AgentGeoType)).setValue(AgentGeoType);
    }

    /**
     * @return the agentGeoFullname
     */
    public String getAgentGeoFullname()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoFullname)).getValue();
    }

    /**
     * @param AgentGeoFullname the AgentGeoFullname to set
     */
    public void setAgentGeoFullname(String AgentGeoFullname)
    {
        if (StringUtils.isNotBlank(AgentGeoFullname))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoFullname)).setValue(AgentGeoFullname);
        }
    }

    /**
     * @return the agentGeoCountryCode
     */
    public String getAgentGeoCountryCode()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoCountryCode)).getValue();
    }

    /**
     * @param AgentGeoCountryCode the AgentGeoCountryCode to set
     */
    public void setAgentGeoCountryCode(String AgentGeoCountryCode)
    {
        if (StringUtils.isNotBlank(AgentGeoCountryCode))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoCountryCode)).setValue(AgentGeoCountryCode);
        }
    }

    /**
     * @return the agentGeoADM1Code
     */
    public String getAgentGeoADM1Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoADM1Code)).getValue();
    }

    /**
     * @param AgentGeoADM1Code the AgentGeoADM1Code to set
     */
    public void setAgentGeoADM1Code(String AgentGeoADM1Code)
    {
        if (StringUtils.isNotBlank(AgentGeoADM1Code))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoADM1Code)).setValue(AgentGeoADM1Code);
        }
    }

    /**
     * @return the agentGeoADM2Code
     */
    public String getAgentGeoADM2Code()
    {
        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoADM2Code)).getValue();
    }

    /**
     * @param AgentGeoADM2Code the AgentGeoADM2Code to set
     */
    public void setAgentGeoADM2Code(String AgentGeoADM2Code)
    {
        if (StringUtils.isNotBlank(AgentGeoADM2Code))
        {
            ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoADM2Code)).setValue(AgentGeoADM2Code);
        }
    }

    /**
     * @return the agentGeoFeatureID
     */
    public String getAgentGeoFeatureID()
    {

        return ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoFeatureID)).getValue();

    }

    /**
     * @param agentGeoFeatureID the agentGeoFeatureID to set
     */
    public void setAgentGeoFeatureID(String agentGeoFeatureID)
    {
        ((CoalesceStringField) this.getFieldByName(GDELTAgentConstants.AgentGeoFeatureID)).setValue(agentGeoFeatureID);

    }

    public Coordinate getAgentGeoLocation()
    {
        try
        {
            return ((CoalesceCoordinateField) this.getFieldByName(GDELTAgentConstants.AgentGeoLocation)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    public void setAgentGeoLocation(Coordinate actionGeoLocation)
    {
        try
        {
            ((CoalesceCoordinateField) this.getFieldByName(GDELTAgentConstants.AgentGeoLocation)).setValue(actionGeoLocation);
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
        }
    }

    public static CoalesceRecordset createRecordset(CoalesceSection section, String pathName)
    {
        CoalesceRecordset recordSet = CoalesceRecordset.create(section, pathName);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentCode, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.AgentName, ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet, GDELTAgentConstants.NameMetaphone,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentCountryCode,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentKnownGroupCode,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentEthnicCode,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentReligion1Code,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentReligion2Code,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentType1Code,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentType2Code,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentType3Code,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentGeoType,ECoalesceFieldDataTypes.INTEGER_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentGeoFullname,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentGeoCountryCode,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentGeoADM1Code,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentGeoADM2Code,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentGeoFeatureID,ECoalesceFieldDataTypes.STRING_TYPE);
        CoalesceFieldDefinition.create(recordSet,GDELTAgentConstants.AgentGeoLocation,ECoalesceFieldDataTypes.GEOCOORDINATE_TYPE);

        return recordSet;
    }

}
