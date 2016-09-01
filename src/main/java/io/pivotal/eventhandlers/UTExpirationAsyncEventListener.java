package io.pivotal.eventhandlers;

import io.pivotal.functions.UTExpirationFunction;

import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Operation;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEvent;
import com.gemstone.gemfire.cache.asyncqueue.AsyncEventListener;
import com.gemstone.gemfire.cache.execute.FunctionService;
import com.gemstone.gemfire.pdx.PdxInstance;

public class UTExpirationAsyncEventListener<K, V> implements AsyncEventListener, Declarable {

	private Cache cache;
	
	private LogWriter logger = null;
	
	public UTExpirationAsyncEventListener() {
		this.cache = CacheFactory.getAnyInstance();
		this.logger = cache.getLogger();
	}

	public void init(Properties props) {}

	public void close() {}


	@SuppressWarnings("unchecked")
	public boolean processEvents(@SuppressWarnings("rawtypes") List<AsyncEvent> entries) {
		
		Region<String, Map<Date, Set<K>>> UTPurgeHelperRegion = cache.getRegion("/UTPurgeHelper");
		
		//logger.info("***********"+"List Size: " + entries.size());
		
		for (AsyncEvent<K, V> e : entries) {
			if (e.getOperation().equals(Operation.CREATE) || e.getOperation().equals(Operation.UPDATE) 
					|| e.getOperation().equals(Operation.PUTALL_CREATE) || e.getOperation().equals(Operation.PUTALL_UPDATE)) {
				
				// Extract fields from JSON
				K key = e.getKey();
				String newVin = (String) ((PdxInstance) e.getDeserializedValue()).getField("vin");
				String newDateTime = (String) ((PdxInstance) e.getDeserializedValue()).getField("capture_datetime");
				String newDate = newDateTime.split("T")[0];
				
				// Build new key for UTPurgeHelper (format will be: [vin|time], e.g. [1FVACWDT39HAJ3771|2016-04-21]
				String purgeKey = newVin + '|' + newDate;
				
				Set<String> vinSet = new HashSet<String>();
				vinSet.add(purgeKey);
				FunctionService.onRegion(UTPurgeHelperRegion).withFilter(vinSet).withArgs(key).execute(UTExpirationFunction.ID);
			}
		}
		
		return true;
	}
}