package imperial.modaclouds.fg.hdb2ldb;

public class DataTransform {

	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if (args.length < 2) {
			System.out.println("Please input HDB address and the interval (ms) to send the data");
			System.exit(-1);
		}
		//String IP = "http://54.75.134.34:3030/ds/query";
		//String ldbURI = "http://54.155.137.28:3030/ds/data";
		
		String IP = "http://"+args[0]+":3030/ds/query";
		String ldbURI = "http://localhost:3030/ds/data";
				
		HDBQuery hdbquery = new HDBQuery(IP,ldbURI,args[1]);
		hdbquery.start();
		
	}
}
