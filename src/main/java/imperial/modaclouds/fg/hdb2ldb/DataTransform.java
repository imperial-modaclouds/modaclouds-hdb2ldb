package imperial.modaclouds.fg.hdb2ldb;

public class DataTransform {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		if (args.length < 2) {
			System.out.println("Please input HDB address, metrics to save and the period to retrieve data.");
			System.exit(-1);
		}
		
		int period = 1000;
		if (args.length == 3) {
			period = Integer.valueOf(args[2]);
		}
		
		String[] metrics = args[1].split(";");
		
		//String IP = "http://54.75.134.34:3030/ds/query";
		//String ldbURI = "http://54.155.137.28:3030/ds/data";
		
		String IP = "http://"+args[0]+":3030/ds/query";
		String ldbURI = "http://localhost:3030/ds/data";
				
		HDBQuery hdbquery = new HDBQuery(IP,ldbURI,metrics,period);
		hdbquery.start();
		
	}
}
