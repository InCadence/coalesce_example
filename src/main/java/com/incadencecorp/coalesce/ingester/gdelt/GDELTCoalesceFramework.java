package com.incadencecorp.coalesce.ingester.gdelt;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.incadencecorp.coalesce.common.exceptions.CoalescePersistorException;
import com.incadencecorp.coalesce.framework.CoalesceFramework;
import com.incadencecorp.coalesce.framework.persistance.ICoalescePersistor;
import com.incadencecorp.coalesce.search.api.ICoalesceSearchPersistor;
import com.incadencecorp.unity.common.connectors.FilePropertyConnector;
import com.incadencecorp.unity.common.factories.PropertyFactory;

public class GDELTCoalesceFramework {
	private CoalesceFramework framework;
	private ICoalesceSearchPersistor persistor;
    private static final Logger LOGGER = LoggerFactory.getLogger(GDELTCoalesceFramework.class);

	public GDELTCoalesceFramework() throws CoalescePersistorException {
        framework = new CoalesceFramework();
        framework.setIsAnsyncUpdates(false);

        // Make sure a configuration property is defined
        if (System.getProperty("COALESCE_CONFIG_LOCATION") == null) {
        	System.setProperty("COALESCE_CONFIG_LOCATION", "conf");
        }
        String proppath = System.getProperty("COALESCE_CONFIG_LOCATION");
        LOGGER.info("Property Path: " + proppath);
        // Create Property Factory        
        PropertyFactory factory = new PropertyFactory(new FilePropertyConnector(proppath));
        
        List<ICoalescePersistor> persisters = new ArrayList<ICoalescePersistor>();

        // Iterate Over Specified Persisters
        for (String persisterClassName : factory.getProperties("persisters.cfg").keySet())
        {
        
            try
            {
                Object persister = ClassLoader.getSystemClassLoader().loadClass(persisterClassName).newInstance();

                if (persister instanceof ICoalescePersistor)
                {
                    // Authoritative persister is the first persister listed
                    // that implements ICoalesceSearchPersistor, all others are
                    // secondary.
                    if (!framework.isInitialized() && persister instanceof ICoalesceSearchPersistor)
                    {
                        framework.setAuthoritativePersistor((ICoalescePersistor) persister);
                        this.persistor = (ICoalesceSearchPersistor) persister;
                    }
                    else
                    {
                        persisters.add((ICoalescePersistor) persister);
                    }

                    LOGGER.warn("(SUCCESS) Loaded Persister ({})", persisterClassName);
                }
                else
                {
                    LOGGER.error("(FAILED) Loading Persister ({}): {}", persisterClassName, "Invalid Implementation");
                    throw new CoalescePersistorException("(FAILED) Initializing Framework");
                }
            }
            catch (ClassNotFoundException | InstantiationException | IllegalAccessException e)
            {
                LOGGER.error("(FAILED) Loading Persister ({})", persisterClassName, e);
                throw new CoalescePersistorException("(FAILED) Initializing Framework");
            }
        }

        if (!framework.isInitialized())
        {
            throw new CoalescePersistorException("(FAILED) Initializing Framework");
        }

        framework.setSecondaryPersistors(persisters.toArray(new ICoalescePersistor[persisters.size()]));
	}
	
	public CoalesceFramework getFramework() {
		return framework;
	}
	
	public ICoalesceSearchPersistor getAuthoritativePersistor() {
		return persistor;
	}

}
