package io.pivotal.eventhandlers;

import io.pivotal.functions.UTExpirationFunction;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.CacheTransactionManager;
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


	public boolean processEvents(@SuppressWarnings("rawtypes") List<AsyncEvent> entries) {
		
		Region<String, Map<Date, Set<K>>> UTPurgeHelperRegion = cache.getRegion("/UTPurgeHelper");
		
		//CacheTransactionManager txManager = cache.getCacheTransactionManager();
		
		//txManager.begin();
		
		logger.info("***********"+"List Size: " + entries.size());
		
		for (@SuppressWarnings("rawtypes") AsyncEvent e : entries) {
			if (e.getOperation().equals(Operation.CREATE)) {
				String newVin = (String) ((PdxInstance) e.getDeserializedValue()).getField("vin");
				Set<String> vinSet = new HashSet<String>();
				vinSet.add(newVin);
				FunctionService.onRegion(UTPurgeHelperRegion).withFilter(vinSet).withArgs(e).execute(UTExpirationFunction.ID);
			}
		}
		
		
		//txManager.commit();
		
		return true;
	}
}