package io.pivotal.functions;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.execute.FunctionException;
import com.gemstone.gemfire.cache.execute.RegionFunctionContext;

public class UTExpirationFunction<K, V> extends FunctionAdapter implements Declarable {
	private static final long serialVersionUID = 1L;

	public static final String ID = "UT-Expiration-Function";

	private Cache cache;
	
	private LogWriter logger;

	public UTExpirationFunction() {
		this.cache = CacheFactory.getAnyInstance();
		this.logger = cache.getLogger();
	}

	@SuppressWarnings("unchecked")
	public synchronized void execute(FunctionContext context) {
		
		if (!(context instanceof RegionFunctionContext)) {
			throw new FunctionException("This is a data aware function, and has to be called using FunctionService.onRegion.");
		}

		// Get attributes from context
		RegionFunctionContext regionContext = (RegionFunctionContext) context;
		
		K key = (K) regionContext.getArguments();
		String purgeKey = (String) regionContext.getFilter().iterator().next();
		Region<String, Set<K>> UTPurgeHelperRegion = regionContext.getDataSet();
		
		try {
			
			// Get key set for this purgeKey
			Set<K> keys = UTPurgeHelperRegion.keySet().contains(purgeKey) 
						  ? UTPurgeHelperRegion.get(purgeKey)
						  : Collections.newSetFromMap(new ConcurrentHashMap<K, Boolean>());

			// Add key to key set
			keys.add(key);

			// put new key set back to region
			UTPurgeHelperRegion.put(purgeKey, keys);
			
		} catch (Exception exception) {
			System.out.println("UTExpirationFunction Error: " + exception.toString());
		} 
		
		context.getResultSender().lastResult(null);
	}

	public String getId() {
		return ID;
	}
	
	public boolean optimizeForWrite() {
	    return true;
	}

	public void init(Properties properties) {}
}
