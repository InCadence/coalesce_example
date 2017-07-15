package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.sql.rowset.CachedRowSet;

import org.apache.commons.codec.language.DoubleMetaphone;
//import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang3.math.NumberUtils;
import org.geotools.data.Query;
import org.geotools.factory.CommonFactoryFinder;
import org.joda.time.DateTime;
import org.opengis.filter.Filter;
import org.opengis.filter.FilterFactory2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalesceDataFormatException;
import com.incadencecorp.coalesce.common.exceptions.CoalesceException;
import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.common.helpers.EntityLinkHelper;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.ELinkTypes;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.incadencecorp.coalesce.ingester.gdelt.GDELTFields.Fields;
import com.vividsolutions.jts.geom.Coordinate;

public class GDELT_Ingester {

    private final static Logger LOGGER = LoggerFactory.getLogger(GDELT_Ingester.class);
    private String currentGlobalID;
    private CoalesceConnection CoalesceConnection;
    private DoubleMetaphone dmp;

    public GDELT_Ingester() throws CoalescePersistorException, SAXException, IOException
    {
        dmp = new DoubleMetaphone();
        currentGlobalID = "0";
        CoalesceConnection = new CoalesceConnection();
        // Register the entities we will use
        GDELTArtifact.registerEntity(CoalesceConnection.getFramework());
        GDELTEvent.registerEntity(CoalesceConnection.getFramework());
        GDELTAgent.registerEntity(CoalesceConnection.getFramework());
    }

    public List<CoalesceEntity> loadRecordsFromFile(File file) throws IOException, CoalescePersistorException, SQLException
    {
        List<CoalesceEntity> entities = new ArrayList<>();
        try (BufferedReader fr = new BufferedReader(new FileReader(file)))
        {
            String line;
            while ((line = fr.readLine()) != null)
            {
                entities.addAll(persistRecordsFromLine(line, file.getName()));
            }
        }
        return entities;
    }

    public int persistRecordsFromFile(File file) throws IOException, SAXException, CoalescePersistorException, SQLException
    {

        int count = 0;
        List<CoalesceEntity> entities = new ArrayList<>();
        try (BufferedReader fr = new BufferedReader(new FileReader(file)))
        {
            String line;
            long beginTime = System.currentTimeMillis();
            while ((line = fr.readLine()) != null)
            {
                try
                {
                    List<CoalesceEntity> generatedEntities = persistRecordsFromLine(line, file.getName());

                    entities.addAll(generatedEntities);
                    count += generatedEntities.size();
                    if (entities.size() > 100)
                    {
                        long beginTime2 = System.currentTimeMillis();

                        // double processRate = (double) count / (double)
                        // processTime * 1000d;

                        CoalesceConnection.getFramework().saveCoalesceEntity(true,
                                                                             entities.toArray(new CoalesceEntity[entities.size()]));
                        entities.clear();
                        long endTime = System.currentTimeMillis();
                        long processTime = beginTime2 - beginTime;
                        //long persistTime = endTime - beginTime2;
                        long totalTime = endTime - beginTime;
                        double persistRate = (double) count / (double) totalTime * 1000d;

                        // LOGGER.info("persist rate of {} entities per second.
                        // Current ms: {}",
                        // persistRate,
                        // System.currentTimeMillis());

                        double percentProcessTime = ((double) processTime / (double) totalTime) * 100.0;

                        // double percentPersistTime = ((double) persistTime /
                        // (double) totalTime) * 100.0;
                        double percentPersistTime = 100.0 - percentProcessTime;
                        // LOGGER.info(" Total Time: {} Process Time: {} Persist
                        // Time: {}", totalTime, processTime, persistTime);
                        LOGGER.info("{}   persist rate of {} entities per second, processTime: {} %   persistTime: {} % .",
                                    count,
                                    Math.round(persistRate),
                                    Math.round(percentProcessTime),
                                    Math.round(percentPersistTime));
                    }
                }
                catch (CoalescePersistorException e)
                {
                    e.printStackTrace();
                }
            }
            if (!entities.isEmpty())
            {
                CoalesceConnection.getFramework().saveCoalesceEntity(true,
                                                                     entities.toArray(new CoalesceEntity[entities.size()]));
                count += entities.size();
            }
        }
        CoalesceConnection.getFramework().close();
        return count;
    }

    private GDELTArtifact createArtifactEntity(String sourceFile, String gdeltLine, String[] fields)
    {
        GDELTArtifact artifact = new GDELTArtifact();
        // Make sure we don't double-initialize the artifact
        if (!artifact.initialize())
        {
            artifact.initialize();
        }
        GDELTArtifactRecord artifactRecord = artifact.getRecord();


        // Populate the Artifact with the raw data and metadata
        artifactRecord.setSourceFileName(sourceFile);
        artifactRecord.setGlobalEventID(Integer.parseInt(fields[Fields.GlobalEventId.value]));
        artifactRecord.setDateIngested(DateTime.now());

        DateTime artifactDate = new DateTime(Integer.parseInt(sourceFile.substring(0, 4)),
                                             Integer.parseInt(sourceFile.substring(4, 6)),
                                             Integer.parseInt(sourceFile.substring(6, 8)),
                                             0,
                                             0);
        artifactRecord.setArtifactDate(artifactDate);

        // Generate an MD5 Sum for the data chunk
        try
        {
            MessageDigest m = MessageDigest.getInstance("MD5");
            m.update(gdeltLine.getBytes());
            byte[] digest = m.digest();
            BigInteger bigInt = new BigInteger(1, digest);
            String hashText = bigInt.toString(16); // We need to zero pad it to
                                                   // get the full 32 chars.
            while (hashText.length() < 32)
            {
                hashText = "0" + hashText;
            }
            artifactRecord.setMd5Sum(hashText);
        }
        catch (NoSuchAlgorithmException e)
        {
            LOGGER.error(e.getMessage(), e);
        }

        return artifact;
    }

    private GDELTAgent checkAgentExists(String name, String code, String countrycode, String fullname)
            throws CoalescePersistorException, SQLException
    {
        FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();
        String props[] = { "objectKey" };

        Filter nameFilter = ff.equals(ff.property(GDELTAgentConstants.AgentName), ff.literal(name));
        Filter codeFilter = ff.equals(ff.property(GDELTAgentConstants.AgentCode), ff.literal(code));
        Filter ccFilter = ff.equals(ff.property(GDELTAgentConstants.AgentCountryCode), ff.literal(countrycode));
        Filter fullnameFilter = ff.equals(ff.property(GDELTAgentConstants.AgentGeoFullname), ff.literal(fullname));
        List<Filter> flist = new ArrayList<Filter>(Arrays.asList(fullnameFilter, nameFilter, codeFilter, ccFilter));
        Filter fullfilter = ff.and(flist);
        LOGGER.debug("Agent Filter: {}", fullfilter.toString());
        Query query = new Query(GDELTAgentConstants.AgentRecordset, fullfilter, props);

        Long beginTime = System.currentTimeMillis();

        CachedRowSet results = CoalesceConnection.getAuthoritativePersistor().search(query).getResults();
        LOGGER.debug("Agent query time: {}", System.currentTimeMillis() - beginTime);

        if (results.next())
        {
            LOGGER.info("Collapesed Agent");
            GDELTAgent entity = (GDELTAgent) CoalesceObjectFactory.createAndLoad(CoalesceConnection.getFramework().getCoalesceEntity(results.getString(AccumuloPersistor.ENTITY_KEY_COLUMN_NAME)));
            if (!entity.isInitialized())
            {
                entity.initialize();
            }
            if (!results.isLast())
            {
                LOGGER.warn("Multiple Agents for {} / {} / {} / {}", name, code, countrycode, fullname);
            }
            results.close();
            return entity;

        }
        else
        {
            return null;
        }

    }

    private GDELTAgent createAgentFromLine(int which, String[] fields)
            throws CoalescePersistorException, SQLException, CoalesceDataFormatException
    {
        GDELTAgent agent = null;
        // Make sure we have a valid agent before we do anything
        if (which == 1)
        {
            if (fields[Fields.Agent1Name.value] == null || fields[Fields.Agent1Name.value].equals("0")
                    || fields[Fields.Agent1Name.value].isEmpty())
            {
                LOGGER.debug("Agent1 name was: " + fields[Fields.Agent1Name.value]);
                return null;
            }
            else
            {
                agent = checkAgentExists(fields[Fields.Agent1Name.value],
                                         fields[Fields.Agent1Code.value],
                                         fields[Fields.Agent1CountryCode.value],
                                         fields[Fields.Agent1Geo_Fullname.value]);
            }

        }
        else
        {
            if (fields[Fields.Agent2Name.value] == null || fields[Fields.Agent2Name.value].equals("0")
                    || fields[Fields.Agent2Name.value].isEmpty())
            {
                LOGGER.debug("Agent2 name was: " + fields[Fields.Agent2Name.value]);
                return null;
            }
            else
            {
                agent = checkAgentExists(fields[Fields.Agent2Name.value],
                                         fields[Fields.Agent2Code.value],
                                         fields[Fields.Agent2CountryCode.value],
                                         fields[Fields.Agent2Geo_Fullname.value]);
            }

        }

        // Agent already exists get out of here and return existing
        if (agent != null) return agent;

        agent = new GDELTAgent();
        // Don't initialize it if it's already initialized
        if (!agent.isInitialized())
        {
            agent.initialize();
        }
        GDELTAgentRecord agentRecord = agent.getRecord();
        if (which == 1)
        {
            agentRecord.setAgentCode(fields[Fields.Agent1Code.value]);
            agentRecord.setAgentName(fields[Fields.Agent1Name.value]);
            agentRecord.setAgentCountryCode(fields[Fields.Agent1CountryCode.value]);
            agentRecord.setAgentKnownGroupCode(fields[Fields.Agent1KnownGroupCode.value]);
            agentRecord.setAgentEthnicCode(fields[Fields.Agent1EthnicCode.value]);
            agentRecord.setAgentReligion1Code(fields[Fields.Agent1Religion1Code.value]);
            agentRecord.setAgentReligion2Code(fields[Fields.Agent1Religion2Code.value]);
            agentRecord.setAgentType1Code(fields[Fields.Agent1Type1Code.value]);
            agentRecord.setAgentType2Code(fields[Fields.Agent1Type2Code.value]);
            agentRecord.setAgentType3Code(fields[Fields.Agent1Type3Code.value]);
            agentRecord.setAgentGeoType(fields[Fields.Agent1Geo_Type.value]);
            agentRecord.setAgentGeoFullname(fields[Fields.Agent1Geo_Fullname.value]);
            agentRecord.setNameMetaphone(dmp.doubleMetaphone(fields[Fields.Agent1Geo_Fullname.value]));
            agentRecord.setAgentGeoCountryCode(fields[Fields.Agent1Geo_CountryCode.value]);
            agentRecord.setAgentGeoADM1Code(fields[Fields.Agent1Geo_ADM1Code.value]);
            agentRecord.setAgentGeoADM2Code(fields[Fields.Agent1Geo_ADM2Code.value]);
            agentRecord.setAgentGeoFeatureID(fields[Fields.Agent1Geo_FeatureID.value]);

            if (!fields[Fields.Agent1Geo_Lat.value].isEmpty() && !fields[Fields.Agent1Geo_Long.value].isEmpty())
            {
                // If fields are out of range set to limits
                double lat = Float.parseFloat(fields[Fields.Agent1Geo_Lat.value]);
                double lon = Float.parseFloat(fields[Fields.Agent1Geo_Long.value]);

                agentRecord.setAgentGeoLocation(new Coordinate((float) lat, (float) lon));
            }
            else
            {
                // Create a dummy record at the south pole
                agentRecord.setAgentGeoLocation(new Coordinate((float) -90.0, (float) -180.0));
            }
        }
        else
        {
            agentRecord.setAgentCode(fields[Fields.Agent2Code.value]);
            agentRecord.setAgentName(fields[Fields.Agent2Name.value]);
            agentRecord.setAgentCountryCode(fields[Fields.Agent2CountryCode.value]);
            agentRecord.setAgentKnownGroupCode(fields[Fields.Agent2KnownGroupCode.value]);
            agentRecord.setAgentEthnicCode(fields[Fields.Agent2EthnicCode.value]);
            agentRecord.setAgentReligion1Code(fields[Fields.Agent2Religion1Code.value]);
            agentRecord.setAgentReligion2Code(fields[Fields.Agent2Religion2Code.value]);
            agentRecord.setAgentType1Code(fields[Fields.Agent2Type1Code.value]);
            agentRecord.setAgentType2Code(fields[Fields.Agent2Type2Code.value]);
            agentRecord.setAgentType3Code(fields[Fields.Agent2Type3Code.value]);
            agentRecord.setAgentGeoType(fields[Fields.Agent2Geo_Type.value]);
            agentRecord.setAgentGeoFullname(fields[Fields.Agent2Geo_Fullname.value]);
            agentRecord.setNameMetaphone(dmp.doubleMetaphone(fields[Fields.Agent1Geo_Fullname.value]));
            agentRecord.setAgentGeoCountryCode(fields[Fields.Agent2Geo_CountryCode.value]);
            agentRecord.setAgentGeoADM1Code(fields[Fields.Agent2Geo_ADM1Code.value]);
            agentRecord.setAgentGeoADM2Code(fields[Fields.Agent2Geo_ADM2Code.value]);
            agentRecord.setAgentGeoFeatureID(fields[Fields.Agent2Geo_FeatureID.value]);

            if (!fields[Fields.Agent2Geo_Lat.value].isEmpty() && !fields[Fields.Agent2Geo_Long.value].isEmpty())
            {
                // If fields are out of range set to limits
                double lat = Float.parseFloat(fields[Fields.Agent2Geo_Lat.value]);
                double lon = Float.parseFloat(fields[Fields.Agent2Geo_Long.value]);

                agentRecord.setAgentGeoLocation(new Coordinate((float) lat, (float) lon));
            }
            else
            {
                // Create a dummy record at the south pole
                agentRecord.setAgentGeoLocation(new Coordinate((float) -90.0, (float) -180.0));
            }
        }
        if ((agent != null) && (agentRecord != null))
        {
            LOGGER.debug("AgentGeo: {}",
                         agentRecord.getFieldByName(GDELTAgentConstants.AgentGeoLocation).getValue().toString());
        }
        return agent;

    }

    private GDELTEvent createEventFromLine(String[] fields) throws CoalesceDataFormatException
    {
        // Generate the DateTime before we start
        int year = NumberUtils.toInt(fields[Fields.DATEADDED.value].substring(0, 4));
        int month = NumberUtils.toInt(fields[Fields.DATEADDED.value].substring(4, 6));
        int day = NumberUtils.toInt(fields[Fields.DATEADDED.value].substring(6, 8));
        int hour = NumberUtils.toInt(fields[Fields.DATEADDED.value].substring(8, 10));
        int min = NumberUtils.toInt(fields[Fields.DATEADDED.value].substring(10, 12));
        int sec = NumberUtils.toInt(fields[Fields.DATEADDED.value].substring(12, 14));
        DateTime dt = new DateTime(year, month, day, hour, min, sec);

        // Event section
        GDELTEvent event = new GDELTEvent();
        if (!event.isInitialized())
        {
            event.initialize();
        }

        GDELTEventRecord eventRecord = event.getRecord();

        eventRecord.setGlobalEventID(Integer.parseInt(fields[Fields.GlobalEventId.value]));
        eventRecord.setDay(Integer.parseInt(fields[Fields.Day.value]));
        eventRecord.setMonthYear(Integer.parseInt(fields[Fields.MonthYear.value]));
        eventRecord.setYear(Integer.parseInt(fields[Fields.Year.value]));
        eventRecord.setFractionDate(Float.parseFloat(fields[Fields.FractionDate.value]));

        eventRecord.setIsRootEvent(Boolean.parseBoolean(fields[Fields.IsRootEvent.value]));
        eventRecord.setEventCode(fields[Fields.EventCode.value]);
        eventRecord.setEventBaseCode(fields[Fields.EventBaseCode.value]);
        eventRecord.setEventRootCode(fields[Fields.EventRootCode.value]);
        eventRecord.setQuadClass(Integer.parseInt(fields[Fields.QuadClass.value]));
        eventRecord.setGoldsteinScale(Float.parseFloat(fields[Fields.GoldsteinScale.value]));
        eventRecord.setNumMentions(Integer.parseInt(fields[Fields.NumMentions.value]));
        eventRecord.setNumSources(Integer.parseInt(fields[Fields.NumSources.value]));
        eventRecord.setNumArticles(Integer.parseInt(fields[Fields.NumArticles.value]));
        eventRecord.setAvgTone(Float.parseFloat(fields[Fields.AvgTone.value]));

        eventRecord.setActionGeoType(Integer.parseInt(fields[Fields.ActionGeo_Type.value]));
        eventRecord.setActionGeoFullname(fields[Fields.ActionGeo_Fullname.value]);
        eventRecord.setActionGeoCountryCode(fields[Fields.ActionGeo_CountryCode.value]);
        eventRecord.setActionGeoADM1Code(fields[Fields.ActionGeo_ADM1Code.value]);
        eventRecord.setActionGeoADM2Code(fields[Fields.ActionGeo_ADM2Code.value]);
        eventRecord.setActionGeoFeatureID(fields[Fields.ActionGeo_FeatureID.value]);
        eventRecord.setDateAdded(Integer.parseInt(fields[Fields.DATEADDED.value]));
        eventRecord.setSourceURL(fields[Fields.SOURCEURL.value]);
        eventRecord.setDateTime(dt);

        if (!fields[Fields.ActionGeo_Lat.value].isEmpty() && !fields[Fields.ActionGeo_Long.value].isEmpty())
        {
            // If fields are out of range set to limits
            double lat = Float.parseFloat(fields[Fields.ActionGeo_Lat.value]);
            double lon = Float.parseFloat(fields[Fields.ActionGeo_Long.value]);

            eventRecord.setActionGeoLocation(new Coordinate((float) lat, (float) lon));
        }
        else
        {
            LOGGER.debug("Empty Lat/Long for Event");
            eventRecord.setActionGeoLocation(new Coordinate((float) -90.0, (float) -180.0));
        }
        LOGGER.debug("Event Geo: {}",
                     eventRecord.getFieldByName(GDELTEventConstants.ActionGeoLocation).getValue().toString());
        return event;
    }

    /**
     * Method to create Entities from version 2.X GDELT data
     * 
     * @param gdeltLine
     * @return GDELT_Entity created from the ingest
     * @throws CoalescePersistorException
     * @throws SQLException
     */
    public List<CoalesceEntity> persistRecordsFromLine(String gdeltLine, String sourceFile)
            throws CoalescePersistorException, SQLException
    {
        List<CoalesceEntity> entities = new ArrayList<>();
        GDELTAgent agent1 = null;
        GDELTAgent agent2 = null;
        GDELTEvent event = null;

        String[] fields = gdeltLine.split(GDELTFields.SplitToken);
        if (fields.length == Fields.values().length)
        {
            currentGlobalID = fields[Fields.GlobalEventId.value];
            FilterFactory2 ff = CommonFactoryFinder.getFilterFactory2();

            Filter idFilter = ff.equals(ff.property(GDELTArtifactConstants.GlobalEventID), ff.literal(currentGlobalID));
            String props[] = { "objectKey" };
            Query query = new Query(GDELTArtifactConstants.GDELTArtifactRecordset, idFilter, 1, props, "GlobalIDQuery");
            Long beginTime = System.currentTimeMillis();
            CachedRowSet results = CoalesceConnection.getAuthoritativePersistor().search(query).getResults();
            LOGGER.debug("Dup query time: {}", System.currentTimeMillis() - beginTime);
            if (results.size() > 0)
            {

                // Already consumed this event, return empty list and exit
                LOGGER.debug("DUPLICATE FOUND! Still don't know why, but we're not going to persist it again.");
                LOGGER.debug("QueryResult: " + results.size());
                return new ArrayList<>();
            }

            GDELTArtifact artifact = createArtifactEntity(sourceFile, gdeltLine, fields);
            try
            {
                agent1 = createAgentFromLine(1, fields);
                agent2 = createAgentFromLine(2, fields);
                event = createEventFromLine(fields);
            }
            catch (CoalescePersistorException | SQLException | CoalesceDataFormatException e)
            {
                LOGGER.error(e.getMessage(), e);

            }
            try
            {
                // Linkage Section

                EntityLinkHelper.linkEntitiesUniDirectional(artifact,
                                                           ELinkTypes.HAS_PRODUCT,
                                                           event,
                                                           ELinkTypes.HAS_PRODUCT.getLabel(),
                                                           false);

                EntityLinkHelper.linkEntitiesUniDirectional(event,
                                                           ELinkTypes.IS_PRODUCT_OF,
                                                           artifact,
                                                           ELinkTypes.IS_PRODUCT_OF.getLabel(),
                                                           false);

                if (agent1 != null)
                {
                	EntityLinkHelper.linkEntitiesUniDirectional(artifact,
                                                               ELinkTypes.HAS_PRODUCT,
                                                               agent1,
                                                               ELinkTypes.HAS_PRODUCT.getLabel(),
                                                               false);
                	EntityLinkHelper.linkEntitiesUniDirectional(event,
                                                               ELinkTypes.HAS_PARTICIPANT,
                                                               agent1,
                                                               ELinkTypes.HAS_PARTICIPANT.getLabel(),
                                                               false);

                	EntityLinkHelper.linkEntitiesUniDirectional(agent1,
                                                               ELinkTypes.IS_A_PARTICIPANT_OF,
                                                               event,
                                                               ELinkTypes.IS_A_PARTICIPANT_OF.getLabel(),
                                                               false);
                	EntityLinkHelper.linkEntitiesUniDirectional(agent1,
                                                               ELinkTypes.IS_PRODUCT_OF,
                                                               artifact,
                                                               ELinkTypes.IS_PRODUCT_OF.getLabel(),
                                                               false);

                    entities.add(agent1);
                }
                if (agent2 != null)
                {
                	EntityLinkHelper.linkEntitiesUniDirectional(event,
                                                               ELinkTypes.HAS_PARTICIPANT,
                                                               agent2,
                                                               ELinkTypes.HAS_PARTICIPANT.getLabel(),
                                                               false);
                	EntityLinkHelper.linkEntitiesUniDirectional(agent2,
                                                               ELinkTypes.IS_A_PARTICIPANT_OF,
                                                               event,
                                                               ELinkTypes.IS_A_PARTICIPANT_OF.getLabel(),
                                                               false);

                	EntityLinkHelper.linkEntitiesUniDirectional(artifact,
                                                               ELinkTypes.HAS_PRODUCT,
                                                               agent2,

                                                               ELinkTypes.HAS_PRODUCT.getLabel(),
                                                               false);
                	EntityLinkHelper.linkEntitiesUniDirectional(agent2,
                                                               ELinkTypes.IS_PRODUCT_OF,
                                                               artifact,
                                                               ELinkTypes.IS_PRODUCT_OF.getLabel(),
                                                               false);

                    entities.add(agent2);
                }
            }
            catch (CoalesceException e)
            {
                LOGGER.error(e.getMessage(), e);
                ;
            }
            entities.add(artifact);
            entities.add(event);
        }
        return entities;
    }

    public static void main(String[] args) throws FileNotFoundException, IOException, CoalescePersistorException,
            SAXException, URISyntaxException, SQLException
    {
        File dataFile;
        if (args.length != 1)
        {
            URL url = GDELT_Ingester.class.getClassLoader().getResource("20160722150000.testdata.CSV");
            dataFile = new File(url.toURI());
            LOGGER.warn("using default file 20160722150000.testdata.CSV from within jar");
        }
        else
        {
            dataFile = new File(args[0]);
        }
        GDELT_Ingester ingester = new GDELT_Ingester();

        int count = ingester.persistRecordsFromFile(dataFile);
        System.out.println("Persisted " + count + " records");

    }
}
