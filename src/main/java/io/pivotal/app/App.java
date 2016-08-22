package io.pivotal.app;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

import com.gemstone.gemfire.cache.Region;
import com.gemstone.gemfire.cache.client.ClientCache;
import com.gemstone.gemfire.cache.client.ClientCacheFactory;
import com.gemstone.gemfire.cache.query.Query;
import com.gemstone.gemfire.cache.query.QueryService;
import com.gemstone.gemfire.cache.query.SelectResults;
import com.gemstone.gemfire.pdx.PdxInstance;

public class App {

	public static <K, V> void main(String args[]) throws Exception {
		// clearRegion();
		// generateData(10);
		queryData();

		// System.out.println("######################################");

		// purgeData();
		// queryData();

	}

	private static <K, V> void clearRegion() {
		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache
				.getRegion("UnitTelemetryHelper");
		Region<Integer, PdxInstance> unitTelemetryRegion = cache
				.getRegion("UnitTelemetry");

		unitTelemetryHelperRegion.removeAll(unitTelemetryHelperRegion
				.keySetOnServer());
		unitTelemetryRegion.removeAll(unitTelemetryRegion.keySetOnServer());
	}

	private static <K, V> void generateData(int num) {
		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<Integer, PdxInstance> unitTelemetryRegion = cache
				.getRegion("UnitTelemetry");

		Random random = new Random();

		System.out.println("Putting Data...");
		long tStart = System.currentTimeMillis();

		for (int i = 0; i < num; i++) {
			String time = "2015-11-1" + random.nextInt(10) + "T"
					+ random.nextInt(24) + ":" + random.nextInt(60) + ":"
					+ random.nextInt(60) + ":" + random.nextInt(1000) + "Z";
			PdxInstance newItem = cache
					.createPdxInstanceFactory("io.pivotal.Json")
					.writeString("vin", "1FVACWDT39HAJ377" + random.nextInt(1))
					.writeString("capture_datetime", time).create();

			unitTelemetryRegion.put(newItem.hashCode(), newItem);
		}

		long tEnd = System.currentTimeMillis();
		long tDelta = tEnd - tStart;
		double elapsedSeconds = tDelta / 1000.0;
		System.out.println("Finished Putting Data. Time elapsed: "
				+ elapsedSeconds);
	}

	private static <K, V> void purgeData() {

		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache
				.getRegion("UnitTelemetryHelper");
		Region<K, V> unitTelemetryRegion = cache.getRegion("UnitTelemetry");

		// For each VIN, purge the old data
		for (String vin : unitTelemetryHelperRegion.keySetOnServer()) {

			// Get the Date Map for this VIN
			Map<Date, Set<K>> dateMap = unitTelemetryHelperRegion.get(vin);

			// Convert to TreeSet
			TreeSet<Date> dates = new TreeSet<Date>(dateMap.keySet());

			// If map has more than 3 dates, need to remove old keys
			while (dateMap.entrySet().size() > 3) {

				// Remove the earliest date, and get all the keys in it
				Set<K> keys = dateMap.remove(dates.pollFirst());

				// Remove above keys in UnitTelemetry
				unitTelemetryRegion.removeAll(keys);
			}

			// Update dateMap for this VIN in UnitTelemetryHelper
			unitTelemetryHelperRegion.put(vin, dateMap);
		}
	}

	@SuppressWarnings({ "unused", "unchecked" })
	private static <K, V> void queryData() throws Exception {

		// Create Client Cache
		ClientCache cache = new ClientCacheFactory()
				.set("cache-xml-file", "config/clientCache.xml")
				.set("log-level", "error").create();

		Region<String, Map<Date, Set<K>>> unitTelemetryHelperRegion = cache
				.getRegion("UnitTelemetryHelper");
		Region<K, V> unitTelemetryRegion = cache.getRegion("UnitTelemetry");

		QueryService queryService = cache.getQueryService();

		Query query = queryService
				.newQuery("SELECT * FROM /UnitTelemetryHelper.values()");

		Object result = query.execute();

		Collection<?> collection = ((SelectResults<?>) result).asList();

		Iterator<?> iter = collection.iterator();

		while (iter.hasNext()) {
			Map<Date, Set<K>> map = (Map<Date, Set<K>>) iter.next();
			int sum = 0;

			for (Date date : map.keySet()) {
				System.out.println("Date: " + date);
				sum += map.get(date).size();
			}

			System.out.println("Total keys in UnitTelemetryHelper : " + sum);
		}

		Query query2 = queryService
				.newQuery("SELECT count(*) FROM /UnitTelemetry");

		Object result2 = query2.execute();

		Collection<?> collection2 = ((SelectResults<?>) result2).asList();

		Iterator<?> iter2 = collection2.iterator();

		while (iter2.hasNext()) {
			System.out.println("Total records in UnitTelemetry: "
					+ iter2.next());
		}

	}
}
