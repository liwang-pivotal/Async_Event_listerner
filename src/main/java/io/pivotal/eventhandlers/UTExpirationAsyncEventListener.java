package io.pivotal.eventhandlers;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.pdx.PdxInstance;

public class UTExpirationAsyncEventListener<K, V> implements AsyncEventListener, Declarable {

	private LogWriter logger = null;

	public void init(Properties props) {}

	public void close() {}

	@SuppressWarnings("unchecked")
	public boolean processEvents(@SuppressWarnings("rawtypes") List<AsyncEvent> entries) {

		Cache cache = CacheFactory.getAnyInstance();
		
		logger = cache.getLogger();
		
		CacheTransactionManager txManager = cache.getCacheTransactionManager();
		
		txManager.begin();
		
		
		for (@SuppressWarnings("rawtypes") AsyncEvent e : entries) {
			if (e.getOperation().equals(Operation.CREATE)) {
				try {
					put(e, cache, logger);
				} catch (Exception exception) {
					exception.printStackTrace();
				}
			}
		}
		
		
		txManager.commit();
		
		return true;
	}

	private synchronized void put(AsyncEvent<K,V> e, Cache cache, LogWriter logger) throws Exception {
		
		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache.getRegion("/UnitTelemetryHelper");
		
		// Extract fields from JSON
		K key = e.getKey();
		String newVin = (String) ((PdxInstance) e.getDeserializedValue()).getField("vin");
		String newDateTime = (String) ((PdxInstance) e.getDeserializedValue()).getField("capture_datetime");

		// Parse date string to Date object
		String newDateString = newDateTime.split("T")[0];
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Date newDate = df.parse(newDateString);

		// Check if helper region already has VIN. If yes, extract its dateMap, otherwise create new dateMap
		Map<Date, Set<K>> dateMap = unitTelemetryHelperRegion.containsKey(newVin) 
									? (Map<Date, Set<K>>) unitTelemetryHelperRegion.get(newVin) 
									: new ConcurrentHashMap<Date, Set<K>>();

		// Check if dateMap already has such date. If yes, extract its Set, otherwise create new Set
		Set<K> keys = dateMap.keySet().contains(newDate) 
					  ? dateMap.get(newDate)
					  : Collections.newSetFromMap(new ConcurrentHashMap<K, Boolean>());

		// Add key to key set
		keys.add(key);

		// put new set to dateMap
		dateMap.put(newDate, keys);

		// put new dateMap to region
		unitTelemetryHelperRegion.put(newVin, dateMap);
	}
}