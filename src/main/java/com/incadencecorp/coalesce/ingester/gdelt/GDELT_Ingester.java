package com.incadencecorp.coalesce.ingester.gdelt;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.commons.codec.language.DoubleMetaphone;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.common.helpers.EntityLinkHelper;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.CoalesceObjectFactory;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceDateTimeField;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntity;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceEntityTemplate;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecord;
import com.incadencecorp.coalesce.framework.datamodel.CoalesceRecordset;
import com.incadencecorp.coalesce.framework.datamodel.ELinkTypes;
import com.incadencecorp.coalesce.framework.persistance.ICoalescePersistor;
import com.incadencecorp.coalesce.framework.persistance.ServerConn;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloPersistor;
import com.incadencecorp.coalesce.framework.persistance.accumulo.AccumuloSettings;
import com.incadencecorp.coalesce.framework.persistance.postgres.PostGreSQLDataConnector;
import com.incadencecorp.coalesce.framework.persistance.postgres.PostGreSQLPersistor;
import com.incadencecorp.coalesce.framework.persistance.postgres.PostGreSQLPersistorExt2;
import com.incadencecorp.oe.common.constants.AgentConstants;
import com.incadencecorp.oe.common.constants.ArtifactConstants;
import com.incadencecorp.oe.common.constants.EventConstants;
import com.incadencecorp.oe.common.constants.OEEntityConstants;
import com.incadencecorp.oe.common.setters.FieldSetter;
import com.incadencecorp.oe.entities.OEAgent;
import com.incadencecorp.oe.entities.OEArtifact;
import com.incadencecorp.oe.entities.OEEvent;
import com.incadencecorp.oe.entities.OEEntity;
import com.incadencecorp.oe.ingest.Ingester;
import com.incadencecorp.oe.records.*;

import com.incadencecorp.oe.ingest.gdelt.artifact.GDELTArtifact;
import com.incadencecorp.oe.ingest.gdelt.artifact.GDELTConstants;
import com.incadencecorp.oe.ingest.gdelt.artifact.GDELTConstants.Fields;


public class GDELT_Ingester {

    private String source = "GDELT";
    private static final Logger LOGGER = LoggerFactory.getLogger(GDELT_Ingester.class);
    private static boolean firstEntity = true;


    /**
     * Method to create Entities from version 2.X GDELT data
     * 
     * @param gdeltLine
     * @return GDELT_Entity created from the ingest
     */
    private List<OEEntity> createV2EntitiesFromLine(String gdeltLine, String sourceFile)
    {

        List<OEEntity> entities = new ArrayList<>();

        GDELTArtifact artifact = new GDELTArtifact();
        artifact.setAttribute("classname", artifact.getClass().getName());
        artifact.setSource(source);
        artifact.setEntityId(artifact.getKey());
        artifact.setEntityIdType("UUID");
        
        CoalesceRecordset gdelt_artifactRecordSet = artifact.getCoalesceRecordsetForNamePath("OEEntity/GDELTArtifactSection/GDELTArtifactRecordset");
        CoalesceRecord gdelt_artifactRecord = gdelt_artifactRecordSet.getItem(0);

        CoalesceRecordset artifactRecordSet = artifact.getCoalesceRecordsetForNamePath("OEEntity/ArtifactSection/ArtifactRecordset");
        CoalesceRecord artifactRecord = artifactRecordSet.getItem(0);


        String[] fields = gdeltLine.split("\t");
        if (fields.length == 61)
        {

            // Generate the DateTime before we start
            int year = NumberUtils.toInt(fields[59].substring(0, 4));
            int month = NumberUtils.toInt(fields[59].substring(4, 6));
            int day = NumberUtils.toInt(fields[59].substring(6, 8));
            int hour = NumberUtils.toInt(fields[59].substring(8, 10));
            int min = NumberUtils.toInt(fields[59].substring(10, 12));
            int sec = NumberUtils.toInt(fields[59].substring(12, 14));
            DateTime dt = new DateTime(year, month, day, hour, min, sec);

            // Populate the Artifact with the raw data and metadata
            FieldSetter.setStringField(gdelt_artifactRecord, GDELTConstants.SourceFileName, sourceFile);
            FieldSetter.setIntegerField(gdelt_artifactRecord, EventConstants.GlobalEventID, fields[0]);
            FieldSetter.setStringField(gdelt_artifactRecord, GDELTConstants.RawText, StringEscapeUtils.escapeJava(gdeltLine));

            ((CoalesceDateTimeField) artifactRecord.getFieldByName("DateIngested")).setValue(DateTime.now());
            DateTime artifactDate = new DateTime(Integer.parseInt(sourceFile.substring(0, 4)),
                                                 Integer.parseInt(sourceFile.substring(4, 6)),
                                                 Integer.parseInt(sourceFile.substring(6, 8)),
                                                 0,
                                                 0);
            ((CoalesceDateTimeField) artifactRecord.getFieldByName("ArtifactDate")).setValue(artifactDate);

            // Generate an MD5 Sum for the data chunk
            try
            {
                MessageDigest m = MessageDigest.getInstance("MD5");
                m.update(gdeltLine.getBytes());
                byte[] digest = m.digest();
                BigInteger bigInt = new BigInteger(1, digest);
                String hashText = bigInt.toString(16); // We need to zero pad it to get the full 32 chars.
                while (hashText.length() < 32)
                {
                    hashText = "0" + hashText;
                }
                FieldSetter.setStringField(artifactRecord, ArtifactConstants.Md5Sum, hashText);
            }
            catch (NoSuchAlgorithmException e)
            {
                e.printStackTrace();
            }
            entities.add(artifact);

            // Actor1 section
            OEAgent agent1 = new OEAgent();

            agent1.setSource(source);
            agent1.setEntityId(agent1.getKey());
            agent1.setEntityIdType("UUID");
            CoalesceRecordset actor1RecordSet = agent1.getCoalesceRecordsetForNamePath("OEEntity/AgentSection/AgentRecordset");
            CoalesceRecord actor1Record = actor1RecordSet.getItem(0);
            CoalesceRecordset actor1OERecordSet = agent1.getCoalesceRecordsetForNamePath(OEEntityConstants.OEEntity + "/"
                    + OEEntityConstants.OESection + "/" + OEEntityConstants.OERecordset);
            CoalesceRecord actor1OERecord = actor1OERecordSet.getItem(0);

            FieldSetter.setBooleanField(actor1OERecord, OEEntityConstants.IsSimulation, false);
            FieldSetter.setStringField(actor1OERecord, OEEntityConstants.DataSource, "GDELT");
            FieldSetter.setStringField(actor1OERecord, OEEntityConstants.OntologyReference, "Actor");

            FieldSetter.setStringField(actor1Record, AgentConstants.AgentCode, fields[5]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentName, fields[6]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentCountryCode, fields[7]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentKnownGroupCode, fields[8]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentEthnicCode, fields[9]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentReligion1Code, fields[10]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentReligion2Code, fields[11]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentType1Code, fields[12]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentType2Code, fields[13]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentType3Code, fields[14]);

            FieldSetter.setIntegerField(actor1Record, AgentConstants.AgentGeoType, fields[35]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentGeoFullname, fields[36]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentGeoCountryCode, fields[37]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentGeoADM1Code, fields[38]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentGeoADM2Code, fields[39]);
            FieldSetter.setStringField(actor1Record, AgentConstants.AgentGeoFeatureID, fields[42]);

            if (!fields[40].isEmpty() && !fields[41].isEmpty())
            {
            	FieldSetter.buildAndSetGeoField(actor1Record,
            			AgentConstants.AgentGeo,
                                            Float.parseFloat(fields[40]),
                                            Float.parseFloat(fields[41]));
            } else {
            	LOGGER.debug("Empty Lat/Long for Actor1");
            }
            entities.add(agent1);

            // Actor2 section
            OEAgent agent2 = new OEAgent();
            agent2.setSource(source);
            agent2.setEntityId(agent2.getKey());
            agent2.setEntityIdType("UUID");
            CoalesceRecordset actor2RecordSet = agent2.getCoalesceRecordsetForNamePath("OEEntity/AgentSection/AgentRecordset");
            CoalesceRecord actor2Record = actor2RecordSet.getItem(0);
            CoalesceRecordset actor2OERecordSet = agent2.getCoalesceRecordsetForNamePath(OEEntityConstants.OEEntity + "/"
                    + OEEntityConstants.OESection + "/" + OEEntityConstants.OERecordset);
            CoalesceRecord actor2OERecord = actor2OERecordSet.getItem(0);

            FieldSetter.setBooleanField(actor2OERecord, OEEntityConstants.IsSimulation , false);
            FieldSetter.setStringField(actor2OERecord, OEEntityConstants.DataSource, "GDELT");
            FieldSetter.setStringField(actor1OERecord, OEEntityConstants.OntologyReference, "Agent");

            FieldSetter.setStringField(actor2Record, AgentConstants.AgentCode, fields[15]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentName, fields[16]);
            FieldSetter.setStringField(actor2Record, AgentConstants.NameMetaphone, new DoubleMetaphone().doubleMetaphone(fields[16]));
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentCountryCode, fields[17]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentKnownGroupCode, fields[18]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentEthnicCode, fields[19]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentReligion1Code, fields[20]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentReligion2Code, fields[21]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentType1Code, fields[22]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentType2Code, fields[23]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentType3Code, fields[24]);

            FieldSetter.setIntegerField(actor2Record, AgentConstants.AgentGeoType, fields[43]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentGeoFullname, fields[44]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentGeoCountryCode, fields[45]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentGeoADM1Code, fields[46]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentGeoADM2Code, fields[47]);
            FieldSetter.setStringField(actor2Record, AgentConstants.AgentGeoFeatureID, fields[50]);

            if (!fields[48].isEmpty() && !fields[49].isEmpty())
            {
            	FieldSetter.buildAndSetGeoField(actor2Record,
            				AgentConstants.AgentGeo,
                            Float.parseFloat(fields[48]),
                            Float.parseFloat(fields[49]));
            } else {
            	LOGGER.debug("Empty Lat/Long for Actor2");
            }
            entities.add(agent2);

            // Event section
            OEEvent event = new OEEvent();

            event.setSource(source);
            event.setEntityId(event.getKey());
            event.setEntityIdType("UUID");
            CoalesceRecordset eventRecordSet = event.getCoalesceRecordsetForNamePath("OEEntity/EventSection/EventRecordset");
            CoalesceRecord eventRecord = eventRecordSet.getItem(0);
            CoalesceRecordset eventOERecordSet = event.getCoalesceRecordsetForNamePath(OEEntityConstants.OEEntity + "/"
                    + OEEntityConstants.OESection + "/" + OEEntityConstants.OERecordset);
            CoalesceRecord eventOERecord = eventOERecordSet.getItem(0);

            FieldSetter.setBooleanField(eventOERecord, OEEntityConstants.IsSimulation, false);
            FieldSetter.setStringField(eventOERecord, OEEntityConstants.DataSource, "GDELT");
            FieldSetter.setStringField(actor1OERecord, OEEntityConstants.OntologyReference, "Event");

            FieldSetter.setIntegerField(eventRecord, EventConstants.GlobalEventID, fields[0]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.Day, fields[1]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.MonthYear, fields[2]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.Year, fields[3]);
            FieldSetter.setFloatField(eventRecord, EventConstants.FractionDate, fields[4]);

            FieldSetter.setIntegerField(eventRecord, EventConstants.IsRootEvent, fields[25]);
            FieldSetter.setStringField(eventRecord, EventConstants.EventCode, fields[26]);
            FieldSetter.setStringField(eventRecord, EventConstants.EventBaseCode, fields[27]);
            FieldSetter.setStringField(eventRecord, EventConstants.EventRootCode, fields[28]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.QuadClass, fields[29]);
            FieldSetter.setFloatField(eventRecord, EventConstants.GoldsteinScale, fields[30]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.NumMentions, fields[30]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.NumSources, fields[31]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.NumArticles, fields[32]);
            FieldSetter.setFloatField(eventRecord, EventConstants.AvgTone, fields[34]);

            FieldSetter.setIntegerField(eventRecord, EventConstants.ActionGeoType, fields[51]);
            FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoFullname, fields[52]);
            FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoCountryCode, fields[53]);
            FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoADM1Code, fields[54]);
            FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoADM2Code, fields[55]);
            FieldSetter.setStringField(eventRecord, EventConstants.ActionGeoFeatureID, fields[58]);
            FieldSetter.setIntegerField(eventRecord, EventConstants.DateAdded, fields[59]);
            FieldSetter.setStringField(eventRecord, EventConstants.SourceURL, fields[60]);
            ((CoalesceDateTimeField) eventRecord.getFieldByName(EventConstants.DateTime)).setValue(dt);

            if (!fields[56].isEmpty() && !fields[57].isEmpty())
            {
            	FieldSetter.buildAndSetGeoField(eventRecord,
                		EventConstants.ActionGeo,
                                            Float.parseFloat(fields[56]),
                                            Float.parseFloat(fields[57]));
            } else {
            	LOGGER.debug("Empty Lat/Long for Event");
            }

            // Linkage Section
            // TODO: Use the non-deprecated version with all the extra stuff
            //EntityLinkHelper.linkEntities(artifact, ELinkTypes.HAS_PRODUCT, agent1, false);
            //EntityLinkHelper.linkEntities(artifact, ELinkTypes.HAS_PRODUCT, agent2, false);
            //EntityLinkHelper.linkEntities(artifact, ELinkTypes.HAS_PRODUCT, event, false);

            //EntityLinkHelper.linkEntities(event, ELinkTypes.IS_PRODUCT_OF, artifact, false);
            EntityLinkHelper.linkEntities(event, ELinkTypes.HAS_PARTICIPANT, agent1, false);
            EntityLinkHelper.linkEntities(event, ELinkTypes.HAS_PARTICIPANT, agent2, false);

            //EntityLinkHelper.linkEntities(agent1, ELinkTypes.IS_PRODUCT_OF, artifact, false);
            EntityLinkHelper.linkEntities(agent1, ELinkTypes.IS_A_PARTICIPANT_OF, event, false);

            //EntityLinkHelper.linkEntities(agent2, ELinkTypes.IS_PRODUCT_OF, artifact, false);
            EntityLinkHelper.linkEntities(agent2, ELinkTypes.IS_A_PARTICIPANT_OF, event, false);

            // LOGGER.info("adding event entity");
            entities.add(event);

        }
        return entities;
    }

    public List<CoalesceEntity> loadRecordsFromFile(File file) throws IOException
    {
        List<CoalesceEntity> entities = new ArrayList<>();
        try (BufferedReader fr = new BufferedReader(new FileReader(file)))
        {
            String line;
            while ((line = fr.readLine()) != null)
            {
                entities.addAll(createV2EntitiesFromLine(line, file.getName()));
            }
        }
        return entities;
    }

    public int persistRecordsFromFile(ICoalescePersistor persistor, File file)
            throws IOException, SAXException, CoalescePersistorException
    {

        CoalesceFramework coalesceFramework = new CoalesceFramework();
        coalesceFramework.setAuthoritativePersistor(persistor);
        if (firstEntity) {
            CoalesceObjectFactory.register(GDELTArtifact.class);
            CoalesceObjectFactory.register(OEEntity.class);
            CoalesceObjectFactory.register(OEAgent.class);
            // TODO Figure out how to do the template registration better - maybe by standard we should do it in
            // TODO the entity class
            GDELTArtifact artifact = new GDELTArtifact();
            artifact.setSource(source);
            
            CoalesceRecordset gdelt_artifactRecordSet = artifact.getCoalesceRecordsetForNamePath("OEEntity/GDELTArtifactSection/GDELTArtifactRecordset");
            CoalesceRecord gdelt_artifactRecord = gdelt_artifactRecordSet.getItem(0);
            String recname = gdelt_artifactRecord.getName();
 //           gdelt_artifactRecord.setName("GDELTArtifact");
            
            CoalesceEntityTemplate arttemplate = CoalesceEntityTemplate.create(artifact);
            //framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(artifact));
            OEAgent agent1 = new OEAgent();
            agent1.setSource(source);
            CoalesceRecordset agent1RecordSet = agent1.getCoalesceRecordsetForNamePath("OEEntity/AgentSection/AgentRecordset");
            CoalesceRecord agentRecord = agent1RecordSet.getItem(0);
            String arecname = agentRecord.getName();
            CoalesceEntityTemplate actortemplate=CoalesceEntityTemplate.create(agent1);
            //framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(agent1));
            OEEvent event = new OEEvent();
            event.setSource(source);
            CoalesceRecordset eventRecordSet = event.getCoalesceRecordsetForNamePath("OEEntity/EventSection/EventRecordset");
            CoalesceRecord eventRecord = eventRecordSet.getItem(0);
            CoalesceEntityTemplate eventtemplate=CoalesceEntityTemplate.create(event);
            //framework.saveCoalesceEntityTemplate(CoalesceEntityTemplate.create(event));
            coalesceFramework.saveCoalesceEntityTemplate(arttemplate, eventtemplate, actortemplate);
            persistor.registerTemplate(arttemplate, eventtemplate, actortemplate);
            firstEntity=false;
        }
 
        
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
                    List<OEEntity> generatedEntities = createV2EntitiesFromLine(line, file.getName());

                    entities.addAll(generatedEntities);
                    count += generatedEntities.size();
                    if (entities.size() > 100)
                    {
                        long beginTime2 = System.currentTimeMillis();

                        
                        //double processRate = (double) count / (double) processTime * 1000d;

                        persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
                        entities.clear();
                        long endTime = System.currentTimeMillis();
                        long processTime = beginTime2 - beginTime;
                        long persistTime = endTime - beginTime2;
                        long totalTime = endTime - beginTime;
                        double persistRate = (double) count / (double) totalTime * 1000d;

                        
                        
//                        LOGGER.info("persist rate of {} entities per second.   Current ms: {}",
//                                    persistRate,
//                                    System.currentTimeMillis());

                        
                        double percentProcessTime = ((double) processTime / (double) totalTime) * 100.0;

                        //double percentPersistTime = ((double) persistTime / (double) totalTime) * 100.0;
                        double percentPersistTime = 100.0 - percentProcessTime;
                        //LOGGER.info(" Total Time: {}   Process Time: {}    Persist Time: {}", totalTime, processTime, persistTime);
                        LOGGER.info("{}   persist rate of {} entities per second, processTime: {} %   persistTime: {} % .", count, 
                        	 Math.round(persistRate),
                             Math.round(percentProcessTime), Math.round(percentPersistTime));

                    }
                }
                catch (CoalescePersistorException e)
                {
                    e.printStackTrace();
                }
            }
            if (!entities.isEmpty())
            {
                persistor.saveEntity(true, entities.toArray(new CoalesceEntity[entities.size()]));
                count += entities.size();
            }
        }
        coalesceFramework.close();
        return count;
    }

    public static void main(String[] args)
            throws FileNotFoundException, IOException, CoalescePersistorException, SAXException
    {
        if (args.length != 2)
        {

            for (String arg : args)
            {
                System.out.println("arg: " + arg);
            }

            System.out.println("Usage: java -cp $CLASSPATH com.incadencecorp.coalesce.ingester.gdelt.GDELT_Ingester configfile datafile");
        }
        else
        {

            GDELT_Ingester ingester = new GDELT_Ingester();
            Properties props = new Properties();
            props.load(new FileReader(new File(args[0])));
            File inputFile = new File(args[1]);

            String dbName = props.getProperty("database");
            String zookeepers = props.getProperty("zookeepers");
            String user = props.getProperty("userid");
            String password = props.getProperty("password");
            LOGGER.info("DB: {}  Zoo/Host: {} User: {}, Passwd: {}", dbName,zookeepers,user,password); 

           //AccumuloSettings.setPersistFieldDefAttr(false);
           //AccumuloSettings.setPersistSectionAttr(false);
           //AccumuloSettings.setPersistRecordsetAttr(false);
           //AccumuloSettings.setPersistRecordAttr(false);
           
            ServerConn conn = new ServerConn.Builder().db(dbName).serverName(zookeepers).user(user).password(password).build();
            conn.setUser(user);
            conn.setPassword(password);
            //AccumuloPersistor persistor = new AccumuloPersistor(conn);
            PostGreSQLPersistorExt2 persistor = new PostGreSQLPersistorExt2();
            persistor.setConnectionSettings(conn);
            persistor.setSchema("coalesce");
            int count = ingester.persistRecordsFromFile(persistor, inputFile);
            System.out.println("Persisted " + count + " records");
            //persistor.close();
        }
    }
}
