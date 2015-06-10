package imperial.modaclouds.fg.hdb2ldb;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class DataTransform {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		int period = 1000;
		if (args.length == 1) {
			period = Integer.valueOf(args[0]);
		}
		
		String HDBIP = System.getenv("MODACLOUDS_HISTORYDB_ENDPOINT_IP");
		String HDBPort = System.getenv("MODACLOUDS_HISTORYDB_ENDPOINT_PORT");
		
	    String OS_IP = System.getenv("MOSAIC_OBJECT_STORE_ENDPOINT_IP");
	    String OS_PORT = System.getenv("MOSAIC_OBJECT_STORE_ENDPOINT_PORT");
	    String OS_FG_PATH = System.getenv("MOSAIC_OBJECT_STORE_FG_PATH");
	    String OS_METRICS = System.getenv("MOSAIC_OBJECT_STORE_FG_LOCALDB_METRICS");
		
	    String command = "curl -X GET http://"+OS_IP+":"+OS_PORT+OS_FG_PATH+OS_METRICS;
	    
	    Runtime r = Runtime.getRuntime();
	    Process p;
	    String metric = null;
		try {
			p = r.exec(command);
			p.waitFor();
		    BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
		    String line = "";

		    while ((line = b.readLine()) != null) {
		    	metric = line;
		    	break;
		    }

		    b.close();
		} catch (Exception e) {
			e.printStackTrace();
			System.out.println("Unable to obtain the metrics to retrieve from object store");
		} 
	    
	    System.out.println(metric);
		String[] metrics = metric.split(";");
		
		//String IP = "http://54.75.134.34:3030/ds/query";
		//String ldbURI = "http://54.155.137.28:3030/ds/data";
		
		String IP = "http://"+HDBIP+":"+HDBPort+"3030/ds/query";
		String ldbURI = "http://localhost:3030/ds/data";
				
		HDBQuery hdbquery = new HDBQuery(IP,ldbURI,metrics,period);
		hdbquery.start();
		
	}
}
