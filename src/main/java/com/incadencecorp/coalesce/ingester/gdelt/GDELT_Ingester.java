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
import org.apache.commons.lang.StringEscapeUtils;
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
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.ELinkTypes;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.incadencecorp.coalesce.ingester.gdelt.GDELTFields.Fields;

public class GDELT_Ingester {
    
    public GDELT_Ingester() throws CoalescePersistorException
    {
	    dmp = new DoubleMetaphone();
	    currentGlobalID = "0";
        gdeltCoalesceFramework = new GDELTCoalesceFramework();
    }

    private String source = "GDELT";
	private final static Logger LOGGER = LoggerFactory.getLogger(GDELT_Ingester.class);
	private boolean firstEntity = true;
    private String currentGlobalID;
    private DoubleMetaphone dmp;
    private GDELTCoalesceFramework gdeltCoalesceFramework;

	public List<CoalesceEntity> loadRecordsFromFile(File file) throws IOException, CoalescePersistorException, SQLException {
		List<CoalesceEntity> entities = new ArrayList<>();
		try (BufferedReader fr = new BufferedReader(new FileReader(file))) {
			String line;
			while ((line = fr.readLine()) != null) {
				entities.addAll(persistRecordsFromLine(line, file.getName()));
			}
		}
		return entities;
	}

	public int persistRecordsFromFile(File file)
			throws IOException, SAXException, CoalescePersistorException, SQLException {
		if (firstEntity) {
			CoalesceObjectFactory.register(GDELTArtifact.class);
			CoalesceObjectFactory.register(GDELTEntity.class);
			CoalesceObjectFactory.register(GDELTAgent.class);
			// TODO Figure out how to do the template registration better -
			// maybe by standard we should do it in
			// TODO the entity class
			GDELTArtifact artifact = new GDELTArtifact();
			artifact.setSource(source);

			CoalesceRecordset gdelt_artifactRecordSet = artifact
					.getCoalesceRecordsetForNamePath("GDELTEntity/GDELTArtifactSection/GDELTArtifactRecordset");
			CoalesceRecord gdelt_artifactRecord = gdelt_artifactRecordSet.getItem(0);
			String recname = gdelt_artifactRecord.getName();
			// gdelt_artifactRecord.setName("GDELTArtifact");

			CoalesceEntityTemplate arttemplate = CoalesceEntityTemplate.create(artifact);
			// framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(artifact));
			GDELTAgent agent1 = new GDELTAgent();
			agent1.setSource(source);
			CoalesceRecordset agent1RecordSet = agent1
					.getCoalesceRecordsetForNamePath("GDELTEntity/AgentSection/AgentRecordset");
			CoalesceRecord agentRecord = agent1RecordSet.getItem(0);
			String arecname = agentRecord.getName();
			CoalesceEntityTemplate agenttemplate = CoalesceEntityTemplate.create(agent1);
			// framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(agent1));
			GDELTEvent event = new GDELTEvent();
			event.setSource(source);
			CoalesceRecordset eventRecordSet = event
					.getCoalesceRecordsetForNamePath("GDELTEntity/EventSection/EventRecordset");
			CoalesceRecord eventRecord = eventRecordSet.getItem(0);
			CoalesceEntityTemplate eventtemplate = CoalesceEntityTemplate.create(event);
			// framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(event));
			gdeltCoalesceFramework.getFramework().saveCoalesceEntityTemplate(arttemplate, eventtemplate, agenttemplate);
			gdeltCoalesceFramework.getFramework().registerTemplates(arttemplate, eventtemplate, agenttemplate);
			firstEntity = false;
		}

		int count = 0;
		List<CoalesceEntity> entities = new ArrayList<>();
		try (BufferedReader fr = new BufferedReader(new FileReader(file))) {
			String line;
			long beginTime = System.currentTimeMillis();
			while ((line = fr.readLine()) != null) {
				try {
					List<CoalesceEntity> generatedEntities = persistRecordsFromLine(line, file.getName());

					entities.addAll(generatedEntities);
					count += generatedEntities.size();
					if (entities.size() > 100) {
						long beginTime2 = System.currentTimeMillis();

						// double processRate = (double) count / (double)
						// processTime * 1000d;

						gdeltCoalesceFramework.getFramework().saveCoalesceEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
						entities.clear();
						long endTime = System.currentTimeMillis();
						long processTime = beginTime2 - beginTime;
						long persistTime = endTime - beginTime2;
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
						LOGGER.info(
								"{}   persist rate of {} entities per second, processTime: {} %   persistTime: {} % .",
								count, Math.round(persistRate), Math.round(percentProcessTime),
								Math.round(percentPersistTime));
					}
				} catch (CoalescePersistorException e) {
					e.printStackTrace();
				}
			}
			if (!entities.isEmpty()) {
			    gdeltCoalesceFramework.getFramework().saveCoalesceEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
				count += entities.size();
			}
		}
		gdeltCoalesceFramework.getFramework().close();
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
        artifact.setSource(GDELTConstants.Source);
        CoalesceRecordset gdelt_artifactRecordSet = artifact.getCoalesceRecordsetForNamePath(GDELTEntityConstants.GDELTEntity
                + File.separator + GDELTConstants.GDELTArtifactSection + File.separator
                + GDELTConstants.GDELTArtifactRecordset);
        CoalesceRecord gdelt_artifactRecord = gdelt_artifactRecordSet.getItem(0);

        // Populate the Artifact with the raw data and metadata
        FieldSetter.setStringField(gdelt_artifactRecord, GDELTConstants.SourceFileName, sourceFile);
        FieldSetter.setIntegerField(gdelt_artifactRecord, GDELTConstants.GlobalEventID, fields[Fields.GlobalEventId.value]);
        FieldSetter.setStringField(gdelt_artifactRecord, GDELTConstants.RawText, StringEscapeUtils.escapeJava(gdeltLine));

        ((CoalesceDateTimeField) gdelt_artifactRecord.getFieldByName(GDELTConstants.DateIngested)).setValue(DateTime.now());
        DateTime artifactDate = new DateTime(Integer.parseInt(sourceFile.substring(0, 4)),
                                             Integer.parseInt(sourceFile.substring(4, 6)),
                                             Integer.parseInt(sourceFile.substring(6, 8)),
                                             0,
                                             0);
        ((CoalesceDateTimeField) gdelt_artifactRecord.getFieldByName(GDELTConstants.ArtifactDate)).setValue(artifactDate);

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
            FieldSetter.setStringField(gdelt_artifactRecord, GDELTConstants.Md5Sum, hashText);
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
        String props[] = {
                           "objectKey"
        };

        Filter nameFilter = ff.equals(ff.property(AgentConstants.AgentName), ff.literal(name));
        Filter codeFilter = ff.equals(ff.property(AgentConstants.AgentCode), ff.literal(code));
        Filter ccFilter = ff.equals(ff.property(AgentConstants.AgentCountryCode), ff.literal(countrycode));
        Filter fullnameFilter = ff.equals(ff.property(AgentConstants.AgentGeoFullname), ff.literal(fullname));
        List<Filter> flist = new ArrayList<Filter>(Arrays.asList(fullnameFilter, nameFilter, codeFilter, ccFilter));
        Filter fullfilter = ff.and(flist);
        LOGGER.debug("Agent Filter: {}", fullfilter.toString());
        Query query = new Query(AgentConstants.AgentRecordset, fullfilter, props);

        Long beginTime = System.currentTimeMillis();

        CachedRowSet results = gdeltCoalesceFramework.getAuthoritativePersistor().search(query).getResults();
        LOGGER.debug("Agent query time: {}", System.currentTimeMillis() - beginTime);
        // LOGGER.debug("Searching for {} / {} / {} / {}",
        // name,code,countrycode,fullname);
        if (results.next())
        {
            LOGGER.info("Collapesed Agent");
            GDELTAgent entity = (GDELTAgent) CoalesceObjectFactory.createAndLoad(gdeltCoalesceFramework.getFramework().getCoalesceEntity(results.getString(AccumuloPersistor.ENTITY_KEY_COLUMN_NAME)));
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
        if (agent != null)
            return agent;

        agent = new GDELTAgent();
        // Don't initialize it if it's already initialized
        if (!agent.isInitialized())
        {
            agent.initialize();
        }
        CoalesceRecord agentRecord = null;
        CoalesceRecordset agentRecordSet = agent.getCoalesceRecordsetForNamePath(GDELTEntityConstants.GDELTEntity + File.separator
                + AgentConstants.AgentSection + File.separator + AgentConstants.AgentRecordset);
        agentRecord = agentRecordSet.getItem(0);
        CoalesceRecordset agentGDELTRecordSet = agent.getCoalesceRecordsetForNamePath(GDELTEntityConstants.GDELTEntity
                + File.separator + GDELTEntityConstants.GDELTSection + File.separator + GDELTEntityConstants.GDELTRecordset);
        CoalesceRecord agentGDELTRecord = agentGDELTRecordSet.getItem(0);

        FieldSetter.setBooleanField(agentGDELTRecord, GDELTEntityConstants.IsSimulation, false);
        FieldSetter.setStringField(agentGDELTRecord, GDELTEntityConstants.DataSource, GDELTConstants.Source);
        FieldSetter.setStringField(agentGDELTRecord, GDELTEntityConstants.OntologyReference, "Agent");
        // Process different offsets depending on which agent.
        // TODO Figure out a way to collapse this big IF statement
        if (which == 1)
        {
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentCode, fields[Fields.Agent1Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentName, fields[Fields.Agent1Name.value]);
            // LOGGER.debug("DMP P: "+
            // dmp.doubleMetaphone(fields[Fields.Agent1Name.value])+ " DMP A: "
            // +
            // dmp.doubleMetaphone(fields[Fields.Agent1Name.value],true));
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.NameMetaphone,
                                       dmp.doubleMetaphone(fields[Fields.Agent1Name.value]));
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentCountryCode, fields[Fields.Agent1CountryCode.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentKnownGroupCode,
                                       fields[Fields.Agent1KnownGroupCode.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentEthnicCode, fields[Fields.Agent1EthnicCode.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentReligion1Code,
                                       fields[Fields.Agent1Religion1Code.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentReligion2Code,
                                       fields[Fields.Agent1Religion2Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentType1Code, fields[Fields.Agent1Type1Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentType2Code, fields[Fields.Agent1Type2Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentType3Code, fields[Fields.Agent1Type3Code.value]);

            FieldSetter.setIntegerField(agentRecord, AgentConstants.AgentGeoType, fields[Fields.Agent1Geo_Type.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoFullname,
                                       fields[Fields.Agent1Geo_Fullname.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoCountryCode,
                                       fields[Fields.Agent1Geo_CountryCode.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoADM1Code,
                                       fields[Fields.Agent1Geo_ADM1Code.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoADM2Code,
                                       fields[Fields.Agent1Geo_ADM2Code.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoFeatureID,
                                       fields[Fields.Agent1Geo_FeatureID.value]);

            if (!fields[Fields.Agent1Geo_Lat.value].isEmpty() && !fields[Fields.Agent1Geo_Long.value].isEmpty())
            {
                // If fields are out of range set to limits
                double lat = Float.parseFloat(fields[Fields.Agent1Geo_Lat.value]);
                double lon = Float.parseFloat(fields[Fields.Agent1Geo_Long.value]);

                FieldSetter.buildAndSetGeoField(agentRecord, AgentConstants.AgentGeo, (float) lat, (float) lon);
            }
            else
            {
                // Create a dummy record at the south pole
                FieldSetter.buildAndSetGeoField(agentRecord, AgentConstants.AgentGeo, (float) -90.0, (float) -180.0);
            }
        }
        else
        {
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentCode, fields[Fields.Agent2Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentName, fields[Fields.Agent2Name.value]);
            // LOGGER.info("DMP P: "+
            // dmp.doubleMetaphone(fields[Fields.Agent2Name.value])+ " DMP A: "
            // +
            // dmp.doubleMetaphone(fields[Fields.Agent2Name.value],true));
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.NameMetaphone,
                                       dmp.doubleMetaphone(fields[Fields.Agent2Name.value]));
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentCountryCode, fields[Fields.Agent2CountryCode.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentKnownGroupCode,
                                       fields[Fields.Agent2KnownGroupCode.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentEthnicCode, fields[Fields.Agent2EthnicCode.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentReligion1Code,
                                       fields[Fields.Agent2Religion1Code.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentReligion2Code,
                                       fields[Fields.Agent2Religion2Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentType1Code, fields[Fields.Agent2Type1Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentType2Code, fields[Fields.Agent2Type2Code.value]);
            FieldSetter.setStringField(agentRecord, AgentConstants.AgentType3Code, fields[Fields.Agent2Type3Code.value]);

            FieldSetter.setIntegerField(agentRecord, AgentConstants.AgentGeoType, fields[Fields.Agent2Geo_Type.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoFullname,
                                       fields[Fields.Agent2Geo_Fullname.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoCountryCode,
                                       fields[Fields.Agent2Geo_CountryCode.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoADM1Code,
                                       fields[Fields.Agent2Geo_ADM1Code.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoADM2Code,
                                       fields[Fields.Agent2Geo_ADM2Code.value]);
            FieldSetter.setStringField(agentRecord,
                                       AgentConstants.AgentGeoFeatureID,
                                       fields[Fields.Agent2Geo_FeatureID.value]);

            if (!fields[Fields.Agent2Geo_Lat.value].isEmpty() && !fields[Fields.Agent2Geo_Long.value].isEmpty())
            {
                // If fields are out of range set to limits
                double lat = Float.parseFloat(fields[Fields.Agent2Geo_Lat.value]);
                double lon = Float.parseFloat(fields[Fields.Agent2Geo_Long.value]);

                FieldSetter.buildAndSetGeoField(agentRecord, AgentConstants.AgentGeo, (float) lat, (float) lon);
            }
            else
            {
                // Create a dummy record at the south pole
                FieldSetter.buildAndSetGeoField(agentRecord, AgentConstants.AgentGeo, (float) -90.0, (float) -180.0);
            }
        }
        if ((agent != null) && (agentRecord != null))
        {
            LOGGER.debug("AgentGeo: {}", agentRecord.getFieldByName(AgentConstants.AgentGeoLocation).getValue().toString());
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
        event.setSource(GDELTConstants.Source);
        CoalesceRecordset eventRecordSet = event.getCoalesceRecordsetForNamePath(GDELTEntityConstants.GDELTEntity + File.separator
                + EventConstants.EventSection + File.separator + EventConstants.EventRecordset);
        CoalesceRecord eventRecord = eventRecordSet.getItem(0);
        CoalesceRecordset eventGDELTRecordSet = event.getCoalesceRecordsetForNamePath(GDELTEntityConstants.GDELTEntity
                + File.separator + GDELTEntityConstants.GDELTSection + File.separator + GDELTEntityConstants.GDELTRecordset);
        CoalesceRecord eventGDELTRecord = eventGDELTRecordSet.getItem(0);

        FieldSetter.setBooleanField(eventGDELTRecord, GDELTEntityConstants.IsSimulation, false);
        FieldSetter.setStringField(eventGDELTRecord, GDELTEntityConstants.DataSource, GDELTConstants.Source);
        FieldSetter.setStringField(eventGDELTRecord, GDELTEntityConstants.OntologyReference, "Event");

        FieldSetter.setIntegerField(eventRecord, EventConstants.GlobalEventID, fields[Fields.GlobalEventId.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.Day, fields[Fields.Day.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.MonthYear, fields[Fields.MonthYear.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.Year, fields[Fields.Year.value]);
        FieldSetter.setFloatField(eventRecord, EventConstants.FractionDate, fields[Fields.FractionDate.value]);

        FieldSetter.setIntegerField(eventRecord, EventConstants.IsRootEvent, fields[Fields.IsRootEvent.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.EventCode, fields[Fields.EventCode.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.EventBaseCode, fields[Fields.EventBaseCode.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.EventRootCode, fields[Fields.EventRootCode.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.QuadClass, fields[Fields.QuadClass.value]);
        FieldSetter.setFloatField(eventRecord, EventConstants.GoldsteinScale, fields[Fields.GoldsteinScale.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.NumMentions, fields[Fields.NumMentions.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.NumSources, fields[Fields.NumSources.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.NumArticles, fields[Fields.NumArticles.value]);
        FieldSetter.setFloatField(eventRecord, EventConstants.AvgTone, fields[Fields.AvgTone.value]);

        FieldSetter.setIntegerField(eventRecord, EventConstants.ActionGeoType, fields[Fields.ActionGeo_Type.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoFullname, fields[Fields.ActionGeo_Fullname.value]);
        FieldSetter.setStringField(eventRecord,
                                   EventConstants.ActionGeoCountryCode,
                                   fields[Fields.ActionGeo_CountryCode.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoADM1Code, fields[Fields.ActionGeo_ADM1Code.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoADM2Code, fields[Fields.ActionGeo_ADM2Code.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoFeatureID, fields[Fields.ActionGeo_FeatureID.value]);
        FieldSetter.setIntegerField(eventRecord, EventConstants.DateAdded, fields[Fields.DATEADDED.value]);
        FieldSetter.setStringField(eventRecord, EventConstants.SourceURL, fields[Fields.SOURCEURL.value]);
        ((CoalesceDateTimeField) eventRecord.getFieldByName(EventConstants.DateTime)).setValue(dt);

        if (!fields[Fields.ActionGeo_Lat.value].isEmpty() && !fields[Fields.ActionGeo_Long.value].isEmpty())
        {
            // If fields are out of range set to limits
            double lat = Float.parseFloat(fields[Fields.ActionGeo_Lat.value]);
            double lon = Float.parseFloat(fields[Fields.ActionGeo_Long.value]);

            FieldSetter.buildAndSetGeoField(eventRecord, EventConstants.ActionGeo, (float) lat, (float) lon);
        }
        else
        {
            LOGGER.debug("Empty Lat/Long for Event");
            FieldSetter.buildAndSetGeoField(eventRecord, EventConstants.ActionGeo, (float) -90.0, (float) -180.0);
        }
        LOGGER.debug("Event Geo: {}", eventRecord.getFieldByName(EventConstants.ActionGeoLocation).getValue().toString());
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

            Filter idFilter = ff.equals(ff.property(GDELTConstants.GlobalEventID), ff.literal(currentGlobalID));
            String props[] = {
                               "objectKey"
            };
            Query query = new Query(GDELTConstants.GDELTArtifactRecordset, idFilter, 1, props, "GlobalIDQuery");
            Long beginTime = System.currentTimeMillis();
            CachedRowSet results = gdeltCoalesceFramework.getAuthoritativePersistor().search(query).getResults();
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
                // TODO Auto-generated catch block
                LOGGER.error(e.getMessage(), e);
                ;

            }
            try
            {
                // Linkage Section

                GDELTLinkHelper.linkEntitiesUniDirectional(artifact,
                                                        ELinkTypes.HAS_PRODUCT,
                                                        event,
                                                        ELinkTypes.HAS_PRODUCT.getLabel(),
                                                        false);

                GDELTLinkHelper.linkEntitiesUniDirectional(event,
                                                        ELinkTypes.IS_PRODUCT_OF,
                                                        artifact,
                                                        ELinkTypes.IS_PRODUCT_OF.getLabel(),
                                                        false);

                if (agent1 != null)
                {
                    GDELTLinkHelper.linkEntitiesUniDirectional(artifact,
                                                            ELinkTypes.HAS_PRODUCT,
                                                            agent1,
                                                            ELinkTypes.HAS_PRODUCT.getLabel(),
                                                            false);
                    GDELTLinkHelper.linkEntitiesUniDirectional(event,
                                                            ELinkTypes.HAS_PARTICIPANT,
                                                            agent1,
                                                            ELinkTypes.HAS_PARTICIPANT.getLabel(),
                                                            false);

                    GDELTLinkHelper.linkEntitiesUniDirectional(agent1,
                                                            ELinkTypes.IS_A_PARTICIPANT_OF,
                                                            event,
                                                            ELinkTypes.IS_A_PARTICIPANT_OF.getLabel(),
                                                            false);
                    GDELTLinkHelper.linkEntitiesUniDirectional(agent1,
                                                            ELinkTypes.IS_PRODUCT_OF,
                                                            artifact,
                                                            ELinkTypes.IS_PRODUCT_OF.getLabel(),
                                                            false);

                    entities.add(agent1);
                }
                if (agent2 != null)
                {
                    GDELTLinkHelper.linkEntitiesUniDirectional(event,
                                                            ELinkTypes.HAS_PARTICIPANT,
                                                            agent2,
                                                            ELinkTypes.HAS_PARTICIPANT.getLabel(),
                                                            false);
                    GDELTLinkHelper.linkEntitiesUniDirectional(agent2,
                                                            ELinkTypes.IS_A_PARTICIPANT_OF,
                                                            event,
                                                            ELinkTypes.IS_A_PARTICIPANT_OF.getLabel(),
                                                            false);

                    GDELTLinkHelper.linkEntitiesUniDirectional(artifact,
                                                            ELinkTypes.HAS_PRODUCT,
                                                            agent2,

                                                            ELinkTypes.HAS_PRODUCT.getLabel(),
                                                            false);
                    GDELTLinkHelper.linkEntitiesUniDirectional(agent2,
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


    
	public static void main(String[] args)
			throws FileNotFoundException, IOException, CoalescePersistorException, SAXException, URISyntaxException, SQLException {
		File dataFile;
		if (args.length != 1) {
			URL url = GDELT_Ingester.class.getClassLoader().getResource("20160722150000.testdata.CSV");
			dataFile = new File(url.toURI());
			LOGGER.warn("using default file 20160722150000.testdata.CSV from within jar");
		} else {
			dataFile = new File(args[0]);
		}
		GDELT_Ingester ingester = new GDELT_Ingester();

		int count = ingester.persistRecordsFromFile(dataFile);
		System.out.println("Persisted " + count + " records");

	}
}
