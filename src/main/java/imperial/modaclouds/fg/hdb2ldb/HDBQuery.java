package imperial.modaclouds.fg.hdb2ldb;


import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.query.DatasetAccessor;
import com.hp.hpl.jena.query.DatasetAccessorFactory;
import com.hp.hpl.jena.query.ParameterizedSparqlString;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;
import com.hp.hpl.jena.query.Syntax;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.sparql.engine.http.QueryEngineHTTP;


public class HDBQuery implements Runnable
{
	private String HDBIP = null;

	private String ldbURI = null;
	
	private int interval;

	private Thread hdbt;

	private static final String URI = "http://www.modaclouds.eu/rdfs/1.0/monitoringdata#";
	public OntModel model = ModelFactory.createOntologyModel(OntModelSpec.RDFS_MEM);
	public OntClass MonitoringDatum = makeClass("MonitoringDatum");
	public Property metric = makeProperty("metric");
	public Property value = makeProperty("value");
	public Property aboutResource = makeProperty("resourceId");
	public Property timestamp = makeProperty("timestamp");
	public Property requestClass = makeProperty("requestClass");
	public Property arrivalTime = makeProperty("arrivalTime");
	public Property departureTime = makeProperty("departureTime");

	private static ExecutorService execService = null;

	private DatasetAccessor accessor = null; 
	
	public HDBQuery(String HDBIP, String ldbURI, String interval) {
		this.HDBIP = HDBIP;
		this.ldbURI = ldbURI;
		this.interval = Integer.valueOf(interval);

		if (execService == null) {
			execService = Executors.newCachedThreadPool();
			//execService = new ThreadPoolExecutor(maxThreads, 10, 60L, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(maxThreads, true));
		}
		
		if (accessor == null) {
			accessor = DatasetAccessorFactory.createHTTP(ldbURI);
		}
	}

	public void run() {

		while (!hdbt.isInterrupted()) {

			String jsonMessage;
			jsonMessage = obtainData("CPUUtil");
			saveToLocalDB(jsonMessage);
			
			jsonMessage = obtainData("ResponseInfo");
			saveToLocalDB(jsonMessage);

			try {
				Thread.sleep(5*60*1000);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				break;
			}

		}
	}

	private void saveToLocalDB(String json) {
		//DatasetAccessor accessor = DatasetAccessorFactory.createHTTP(ldbURI);
		String graphInitial = "http://www.modaclouds.eu/historydb/monitoring-data/";

		JSONParser jsonParser = new JSONParser();
		JSONObject jsonObject;
		String graph = null;
		try {
			jsonObject = (JSONObject) jsonParser.parse(json);
			JSONObject result = (JSONObject) jsonObject.get("results");
			JSONArray binding = (JSONArray) result.get("bindings");

			Iterator<JSONObject> iter = binding.iterator();
			String metricName = null;
			String metricID = null;
			String metric_value = null;
			String timestamps = null;
			String vmID = null;

			while( iter.hasNext() ) {
				JSONObject data = iter.next();
				JSONObject gJson = (JSONObject) data.get("g");
				String g = (String) gJson.get("value");
				if (graph == null) {
					graph = g.substring(g.lastIndexOf("/")+1);
				}

				JSONObject sJson = (JSONObject) data.get("s");
				String s = (String) sJson.get("value");
				String metricID_new = s.substring(s.lastIndexOf("/")+1);
				if ( metricID == null ) {
					metricID = metricID_new;
				}
				else {
					if (! metricID.equals(metricID_new)) {
						metricID = metricID_new;

						String monDatumInstanceID = MonitoringDatum + "/"
								+ UUID.randomUUID().toString();

						Model m = ModelFactory.createDefaultModel();
						Resource subject = m.createResource(monDatumInstanceID);
						m.add(m.createLiteralStatement(subject, metric, metricName));
						m.add(m.createLiteralStatement(subject, value, metric_value));
						m.add(m.createLiteralStatement(subject, aboutResource, vmID));
						m.add(m.createLiteralStatement(subject, timestamp, Long.valueOf(timestamps)));						

						System.out.println(metricName+" "+metric_value+" "+vmID+" "+timestamps);
						
//						StringWriter w = new StringWriter();
//						m.write(w,"RDF/JSON");
//						System.out.println(w.toString());

						System.out.println(metricName);
						
						execService.execute(new AddExecutor(graphInitial+graph, m));
						
//						System.out.println(execService.getActiveCount());
//						if (execService.getActiveCount() <= maxThreads) {
//							execService.execute(new AddExecutor(graphInitial+graph, m));
//						}
//						else {
//							System.out.println("too many threads. sleeping...");
//							Thread.sleep(5000);
//						}
						
						//accessor.add(graphInitial+graph,m);

						Thread.sleep(interval);
					}
				}

				JSONObject pJson = (JSONObject) data.get("p");
				String p = (String) pJson.get("value");
				String property = p.substring(p.lastIndexOf("#")+1);

				JSONObject oJson = (JSONObject) data.get("o");
				String temp = (String) oJson.get("value");

				if (property.equals("resourceId")) {
					vmID = temp;
				}

				if (property.equals("metric")) {
					metricName = temp;
				}

				if (property.equals("value")) {
					metric_value = temp;
				}

				if (property.equals("timestamp")) {
					timestamps = temp;
				}
			}
			execService.submit(new AddExecutor("default", defaultGraphStatement(graphInitial,Long.valueOf(graph))));
		} catch (ParseException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		
		//accessor.add(defaultGraphStatement(graphInitial,Long.valueOf(graph)));
		
	}
	
	private String obtainData(String metricnameHDB) {
		if ( !HDBIP.contains("http") ) {
			HDBIP = "http://"+HDBIP;
		}
		
		long tnow = System.currentTimeMillis();
		long tstart = tnow - 5*60*1000;
		
//		String sparqlQuery = "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o} GRAPH ?g { ?s "
//				+ "<http://www.modaclouds.eu/rdfs/1.0/monitoringdata#timestamp> ?t "
//				+ "FILTER (?t >= 1412770500000 && ?t <= 1412771400000) . ?s "
//				+ "<http://www.modaclouds.eu/rdfs/1.0/monitoringdata#metric> \""
//				+ metricnameHDB + "\"^^<http://www.w3.org/2001/XMLSchema#string> } }";
		
		String sparqlQuery = "SELECT ?g ?s ?p ?o WHERE { GRAPH ?g { ?s ?p ?o} GRAPH ?g { ?s "
				+ "<http://www.modaclouds.eu/rdfs/1.0/monitoringdata#timestamp> ?t "
				+ "FILTER (?t >= " + "1412772000000" + "&& ?t <= " + "1412775000000" + ") . ?s "
				+ "<http://www.modaclouds.eu/rdfs/1.0/monitoringdata#metric> \""
				+ metricnameHDB + "\"^^<http://www.w3.org/2001/XMLSchema#string> } }";
		
		Query query = QueryFactory.create(sparqlQuery, Syntax.syntaxARQ);

		QueryExecution qexec = QueryExecutionFactory.sparqlService(HDBIP, query); 

		ResultSet results = qexec.execSelect(); 
		ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
		ResultSetFormatter.outputAsJSON(byteStream, results);

		String json = byteStream.toString();
		
		qexec.close();
		try {
			byteStream.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return json;
	}

	public void start() {
		hdbt = new Thread( this, "hdb-query");
		hdbt.start();
	}

	private Property makeProperty(String string) {
		return model.createProperty(URI + string);
	}

	private OntClass makeClass(String string) {
		return model.createClass(URI + string);
	}

	private Model defaultGraphStatement(String graphUrl, long timestamp) {
		Model m = ModelFactory.createDefaultModel();
		Resource subject = null;
		Property property = null;
		Statement statement = null;

		subject = m.createResource(graphUrl);
		property = m.createProperty("mo:timestamp");
		statement = m.createLiteralStatement(subject, property, timestamp);
		m.add(statement);

		return m;
	}

	private class AddExecutor extends Thread {
		private String graphUri;
		private Model model;

		public AddExecutor(String graphUri, Model model) {
			this.graphUri = graphUri;
			this.model = model;
		}

		public void run() {
			long t1 = System.currentTimeMillis();
			if (graphUri.equals("default"))
				accessor.add(model);
			else
				accessor.add(graphUri, model);
			System.out.println(System.currentTimeMillis()-t1);
/*			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				System.out.println("Error while waiting." + e);
			}*/
		}

	}

}
