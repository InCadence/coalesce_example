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

public class AgentRecord extends GDELTRecord {

    private static final Logger LOGGER = LoggerFactory.getLogger(GDELTRecord.class);

    private static GeometryFactory factory = new GeometryFactory();

    /**
     * @return the agentCode
     */
    public String getAgentCode()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentCode)).getValue();
    }

    /**
     * @param agentCode the agentCode to set
     */
    public void setAgentCode(String agentCode)
    {
        if (StringUtils.isNotBlank(agentCode))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentCode)).setValue(agentCode);
        }
    }

    /**
     * @return the agentName
     */
    public String getAgentName()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentName)).getValue();
    }

    /**
     * @param agentName the agentName to set
     */
    public void setAgentName(String agentName)
    {
        if (StringUtils.isNotBlank(agentName))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentName)).setValue(agentName);
        }
    }

    /**
     * @return the NameMetaphone
     */
    public String getNameMetaphone()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.NameMetaphone)).getValue();
    }

    /**
     * @param NameMetaphone the NameMetaphone to set
     */
    public void setNameMetaphone(String NameMetaphone)
    {
        if (StringUtils.isNotBlank(NameMetaphone))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.NameMetaphone)).setValue(NameMetaphone);
        }
    }

    /**
     * @return the agentCountryCode
     */
    public String getAgentCountryCode()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentCountryCode)).getValue();
    }

    /**
     * @param AgentCountryCode the AgentCountryCode to set
     */
    public void setAgentCountryCode(String AgentCountryCode)
    {
        if (StringUtils.isNotBlank(AgentCountryCode))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentCountryCode)).setValue(AgentCountryCode);
        }
    }

    /**
     * @return the agentKnownGroupCode
     */
    public String getAgentKnownGroupCode()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentKnownGroupCode)).getValue();
    }

    /**
     * @param AgentKnownGroupCode the AgentKnownGroupCode to set
     */
    public void setAgentKnownGroupCode(String AgentKnownGroupCode)
    {
        if (StringUtils.isNotBlank(AgentKnownGroupCode))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentKnownGroupCode)).setValue(AgentKnownGroupCode);
        }
    }

    /**
     * @return the agentEthnicCode
     */
    public String getAgentEthnicCode()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentEthnicCode)).getValue();
    }

    /**
     * @param AgentEthnicCode the AgentEthnicCode to set
     */
    public void setAgentEthnicCode(String AgentEthnicCode)
    {
        if (StringUtils.isNotBlank(AgentEthnicCode))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentEthnicCode)).setValue(AgentEthnicCode);
        }
    }

    /**
     * @return the agentReligion1Code
     */
    public String getAgentReligion1Code()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentReligion1Code)).getValue();
    }

    /**
     * @param AgentReligion1Code the AgentReligion1Code to set
     */
    public void setAgentReligion1Code(String AgentReligion1Code)
    {
        if (StringUtils.isNotBlank(AgentReligion1Code))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentReligion1Code)).setValue(AgentReligion1Code);
        }
    }

    /**
     * @return the agentReligion2Code
     */
    public String getAgentReligion2Code()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentReligion2Code)).getValue();
    }

    /**
     * @param AgentReligion2Code the AgentReligion2Code to set
     */
    public void setAgentReligion2Code(String AgentReligion2Code)
    {
        if (StringUtils.isNotBlank(AgentReligion2Code))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentReligion2Code)).setValue(AgentReligion2Code);
        }
    }

    /**
     * @return the agentType1Code
     */
    public String getAgentType1Code()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentType1Code)).getValue();
    }

    /**
     * @param AgentType1Code the AgentType1Code to set
     */
    public void setAgentType1Code(String AgentType1Code)
    {
        if (StringUtils.isNotBlank(AgentType1Code))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentType1Code)).setValue(AgentType1Code);
        }
    }

    /**
     * @return the agentType2Code
     */
    public String getAgentType2Code()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentType2Code)).getValue();
    }

    /**
     * @param AgentType2Code the AgentType2Code to set
     */
    public void setAgentType2Code(String AgentType2Code)
    {
        if (StringUtils.isNotBlank(AgentType2Code))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentType2Code)).setValue(AgentType2Code);
        }
    }

    /**
     * @return the agentType3Code
     */
    public String getAgentType3Code()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentType3Code)).getValue();
    }

    /**
     * @param AgentType3Code the AgentType3Code to set
     */
    public void setAgentType3Code(String AgentType3Code)
    {
        if (StringUtils.isNotBlank(AgentType3Code))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentType3Code)).setValue(AgentType3Code);
        }
    }

    /**
     * @return the agentGeoType
     */
    public Integer getAgentGeoType()
    {
        try
        {
            return ((CoalesceIntegerField) this.getFieldByName(AgentConstants.AgentGeoType)).getValue();
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
            ((CoalesceIntegerField) this.getFieldByName(AgentConstants.AgentGeoType)).setValue(NumberUtils.toInt(AgentGeoType));
        }
    }

    /**
     * @param AgentGeoType the AgentGeoType to set
     */
    public void setAgentGeoType(int AgentGeoType)
    {
        ((CoalesceIntegerField) this.getFieldByName(AgentConstants.AgentGeoType)).setValue(AgentGeoType);
    }

    /**
     * @return the agentGeoFullname
     */
    public String getAgentGeoFullname()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoFullname)).getValue();
    }

    /**
     * @param AgentGeoFullname the AgentGeoFullname to set
     */
    public void setAgentGeoFullname(String AgentGeoFullname)
    {
        if (StringUtils.isNotBlank(AgentGeoFullname))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoFullname)).setValue(AgentGeoFullname);
        }
    }

    /**
     * @return the agentGeoCountryCode
     */
    public String getAgentGeoCountryCode()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoCountryCode)).getValue();
    }

    /**
     * @param AgentGeoCountryCode the AgentGeoCountryCode to set
     */
    public void setAgentGeoCountryCode(String AgentGeoCountryCode)
    {
        if (StringUtils.isNotBlank(AgentGeoCountryCode))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoCountryCode)).setValue(AgentGeoCountryCode);
        }
    }

    /**
     * @return the agentGeoADM1Code
     */
    public String getAgentGeoADM1Code()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoADM1Code)).getValue();
    }

    /**
     * @param AgentGeoADM1Code the AgentGeoADM1Code to set
     */
    public void setAgentGeoADM1Code(String AgentGeoADM1Code)
    {
        if (StringUtils.isNotBlank(AgentGeoADM1Code))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoADM1Code)).setValue(AgentGeoADM1Code);
        }
    }

    /**
     * @return the agentGeoADM2Code
     */
    public String getAgentGeoADM2Code()
    {
        return ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoADM2Code)).getValue();
    }

    /**
     * @param AgentGeoADM2Code the AgentGeoADM2Code to set
     */
    public void setAgentGeoADM2Code(String AgentGeoADM2Code)
    {
        if (StringUtils.isNotBlank(AgentGeoADM2Code))
        {
            ((CoalesceStringField) this.getFieldByName(AgentConstants.AgentGeoADM2Code)).setValue(AgentGeoADM2Code);
        }
    }

    /**
     * @return the agentGeoFeatureID
     */
    public Coordinate getAgentGeoFeatureID()
    {
        try
        {
            return ((CoalesceCoordinateField) this.getFieldByName(AgentConstants.AgentGeoLocation)).getValue();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * @return the agentGeoFeatureID
     */
    public String getAgentGeoFeatureIDAsString()
    {
        try
        {
            return ((CoalesceCoordinateField) this.getFieldByName(AgentConstants.AgentGeoLocation)).getValue().toString();
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
            return null;
        }
    }

    /**
     * @param agentGeoFeatureID the agentGeoFeatureID to set
     */
    public void setAgentGeoFeatureID(String Agent2Geo_Lat, String Agent2Geo_Long)
    {
        try
        {
            if (!Agent2Geo_Lat.isEmpty() && !Agent2Geo_Long.isEmpty())
            {
                // If fields are out of range set to limits
                double lat = Float.parseFloat(Agent2Geo_Lat);
                double lon = Float.parseFloat(Agent2Geo_Long);

                buildAndSetGeoField(this, AgentConstants.AgentGeo, (float) lat, (float) lon);
            }
            else
            {
                // Create a dummy record at the south pole
                buildAndSetGeoField(this, AgentConstants.AgentGeo, (float) -90.0, (float) -180.0);
            }
        }
        catch (CoalesceDataFormatException e)
        {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * Class that generates a geo field based on a lat and long. Takes in a prefix string and appends "Location" to it, so
     * make sure you have a field named <prefix>Location in your record.
     * 
     * @param agentRecord The record that will store the geo field
     * @param prefix The name of the field to store it in. This method will append "Location" to it before saving
     * @param lat The Latitude geo value
     * @param lon The Longitude geo value
     * @throws CoalesceDataFormatException
     * 
     */
    public static void buildAndSetGeoField(CoalesceRecord agentRecord, String prefix, Float lat, Float lon)
            throws CoalesceDataFormatException
    {
        if ((lat != null) && (lon != null))
        {
            if (Math.abs(lat) > 90 || Math.abs(lon) > 180)
            {
                LOGGER.warn("Coordinates out of range - will normalize: LAT: {} LONG: {}", lat, lon);
            }
            if (lat < -90.0) lat = (float) -90.0;
            if (lat > 90) lat = (float) 90.0;
            if (lon < -180.0) lon = (float) -180.0;
            if (lon > 180) lon = (float) 180.0;
            Point point = factory.createPoint(new Coordinate(lon, lat, 0));
            ((CoalesceCoordinateField) agentRecord.getFieldByName(prefix + "Location")).setValue(point);
        }
        else
            LOGGER.error("Coordinates null: Lat: {} Lon: {}", (lat == null), (lon == null));
    }

    public static CoalesceRecordset createRecordset(CoalesceSection section, String pathName)
    {
        CoalesceRecordset recordSet = CoalesceRecordset.create(section, pathName);
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
