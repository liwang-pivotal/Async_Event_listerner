package io.pivotal.eventhandlers;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.pdx.PdxInstance;

public class UTExpirationAsyncEventListener<K, V> implements AsyncEventListener, Declarable {
	
	private final Object lock = new Object();

	public void init(Properties props) {}

	public void close() {}

	@SuppressWarnings("unchecked")
	public boolean processEvents(@SuppressWarnings("rawtypes") List<AsyncEvent> entries) {

		Cache cache = CacheFactory.getAnyInstance();

		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache.getRegion("/UnitTelemetryHelper");
		
		try {
			for (@SuppressWarnings("rawtypes") AsyncEvent e : entries) {
				if (e.getOperation().equals(Operation.CREATE)) {
					put(e, unitTelemetryHelperRegion);
				}
			}
		} catch (ParseException exception) {
			exception.printStackTrace();
		}

		return true;
	}

	private void put(AsyncEvent<K,V> e, Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion) throws ParseException {
		
		// Extract fields from JSON
		K key = e.getKey();
		String new_vin = (String) ((PdxInstance) e.getDeserializedValue())
				.getField("vin");
		String new_datetime = (String) ((PdxInstance) e.getDeserializedValue())
				.getField("capture_datetime");
		String new_dateString = new_datetime.split("T")[0];

		// Parse Date String to Date object
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
		Date new_date = df.parse(new_dateString);

		Map<Date, Set<K>> dateMap;
		
		synchronized (this) {

			// Place the key in UnitTelemetryHelper
			// case 1: if this VIN is already existed
			if (unitTelemetryHelperRegion.containsKey(new_vin)) {
				
					dateMap = (Map<Date, Set<K>>) unitTelemetryHelperRegion.get(new_vin);
	
					Set<K> keys = dateMap.keySet().contains(new_date) ? dateMap.get(new_date) : Collections.newSetFromMap(new ConcurrentHashMap<K, Boolean>());
	
					keys.add(key);
	
					dateMap.put(new_date, keys);
				
			// case 2: if this VIN is new
			} else {
				// create new date map for this VIN
				dateMap = new ConcurrentHashMap<Date, Set<K>>();
	
				// create new key list and insert key
				Set<K> keys = Collections.newSetFromMap(new ConcurrentHashMap<K, Boolean>());
				keys.add(key);
	
				// place new entry(date, list) to dateMap
				dateMap.put(new_date, keys);
			}

			// PUT new record
			unitTelemetryHelperRegion.put(new_vin, dateMap);
		
		}
	}

}