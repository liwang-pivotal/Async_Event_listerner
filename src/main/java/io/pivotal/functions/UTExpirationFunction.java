package io.pivotal.functions;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.distributed.DistributedLockService;
import com.gemstone.gemfire.pdx.PdxInstance;

public class UTExpirationFunction<K, V> extends FunctionAdapter implements Declarable {
	private static final long serialVersionUID = 1L;

	public static final String ID = "size-function";

	private Cache cache;
	
	private LogWriter logger;

	public UTExpirationFunction() {
		this.cache = CacheFactory.getAnyInstance();
		this.logger = cache.getLogger();
	}

	@SuppressWarnings("unchecked")
	public void execute(FunctionContext context) {
		AsyncEvent<K,V> e = (AsyncEvent<K,V>) context.getArguments();
		
		Region<String, Map<Date, Set<K>>> UTPurgeHelperRegion = this.cache.getRegion("/UTPurgeHelper");
		
		// Extract fields from JSON
		K key = e.getKey();
		String newVin = (String) ((PdxInstance) e.getDeserializedValue()).getField("vin");
		String newDateTime = (String) ((PdxInstance) e.getDeserializedValue()).getField("capture_datetime");
		
		Map<String, Lock> lockMap = new HashMap<String, Lock>();
		if (lockMap.containsKey(newVin)) {
			logger.info("***********" + "Got Lock: " + lockMap.get(newVin));
		} else {
			Lock lock = new ReentrantLock();
			logger.info("***********" + "Create new Lock: " + lock);
			lockMap.put(newVin, lock);
		}
		
		
		try {
			// Parse date string to Date object
			String newDateString = newDateTime.split("T")[0];
			DateFormat df = new SimpleDateFormat("yyyy-MM-dd", Locale.ENGLISH);
			Date newDate;
			newDate = df.parse(newDateString);
			
			// Check if helper region already has VIN. If yes, extract its dateMap, otherwise create new dateMap
			Map<Date, Set<K>> dateMap = UTPurgeHelperRegion.containsKey(newVin) 
										? (Map<Date, Set<K>>) UTPurgeHelperRegion.get(newVin) 
										: new ConcurrentHashMap<Date, Set<K>>();
										
			logger.info("***********"+"Current DateMap: "+dateMap.toString() + "      " + "PDX Instance: " + e.getDeserializedValue());

			// Check if dateMap already has such date. If yes, extract its Set, otherwise create new Set
			Set<K> keys = dateMap.keySet().contains(newDate) 
						  ? dateMap.get(newDate)
						  : Collections.newSetFromMap(new ConcurrentHashMap<K, Boolean>());

			// Add key to key set
			keys.add(key);

			// put new set to dateMap
			dateMap.put(newDate, keys);

			// put new dateMap to region
			UTPurgeHelperRegion.put(newVin, dateMap);
			
		} catch (Exception exception) {
			System.out.println("UTExpirationFunction Error: " + exception.toString());
		} 
		
		context.getResultSender().lastResult(UTPurgeHelperRegion.size());
	}

	public String getId() {
		return ID;
	}
	
	public boolean optimizeForWrite() {
	    return true;
	}

	public void init(Properties properties) {}
}
