package io.pivotal.functions;

import java.util.Iterator;
import java.util.Properties;

import com.gemstone.gemfire.LogWriter;
import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.CacheFactory;
import com.gemstone.gemfire.cache.Declarable;
import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.execute.FunctionAdapter;
import com.gemstone.gemfire.cache.execute.FunctionContext;
import com.gemstone.gemfire.cache.partition.PartitionRegionHelper;

public class UTGetVINsFunction<K, V> extends FunctionAdapter implements Declarable {
	private static final long serialVersionUID = 1L;

	public static final String ID = "UT-Reload-Function";

	private Cache cache;
	
	private LogWriter logger;

	public UTGetVINsFunction() {
		this.cache = CacheFactory.getAnyInstance();
		this.logger = cache.getLogger();
	}
	
	@Override
	public void execute(FunctionContext functionContext) {
		
		Region<K, V> UTRegion = this.cache.getRegion("/UnitTelemetry");
		
		Region<K, V> localUTRegion = PartitionRegionHelper.getLocalPrimaryData(UTRegion);
		
		Iterator<K> itr = localUTRegion.keySet().iterator();
		
		while (itr.hasNext()) {
			K key = itr.next();
			UTRegion.put(key, localUTRegion.get(key));
		}
		
		functionContext.getResultSender().lastResult("UT Region Reload Finished!");
	}
	
	public String getId() {
		return ID;
	}
	
	public boolean optimizeForWrite() {
	    return true;
	}

	public void init(Properties properties) {}

}
