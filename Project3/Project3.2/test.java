import java.io.IOException;
import java.sql.Timestamp;
import java.util.Comparator;
import java.util.HashMap;
import java.util.PriorityQueue;
import java.util.TimeZone;




import org.vertx.java.core.Handler;
import org.vertx.java.core.MultiMap;
import org.vertx.java.core.http.HttpServer;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.platform.Verticle;

public class Coordinator extends Verticle {

	//Default mode: sharding. Possible string values are "replication" and "sharding"
	private static String storageType = "replication";

	/**
	 * TODO: Set the values of the following variables to the DNS names of your
	 * three dataCenter instances
	 */
	private static final String dataCenter1 = "ec2-54-173-201-116.compute-1.amazonaws.com";
	private static final String dataCenter2 = "ec2-54-174-35-239.compute-1.amazonaws.com";
	private static final String dataCenter3 = "ec2-54-173-255-78.compute-1.amazonaws.com";
	

	//this is my hash function
	public String hashFunction(String key) {
		int code  =  key.hashCode();
		if(code%3 == 0) {
			return dataCenter1;
		}else if(code%3 == 1) {
			return dataCenter2;
		}else {
			return dataCenter3;
		}
	}
	
	@Override
	public void start() {
		//DO NOT MODIFY THIS
		KeyValueLib.dataCenters.put(dataCenter1, 1);
		KeyValueLib.dataCenters.put(dataCenter2, 2);
		KeyValueLib.dataCenters.put(dataCenter3, 3);
		final RouteMatcher routeMatcher = new RouteMatcher();
		final HttpServer server = vertx.createHttpServer();
		server.setAcceptBacklog(32767);
		server.setUsePooledBuffers(true);
		server.setReceiveBufferSize(4 * 1024);


		// this is the comparator for the priority queue
		Comparator<String>timestampComparator = new Comparator<String>() {
			@Override
			public int compare(String o1, String o2) {
				return o1.compareTo(o2);
			}
		};
		

		//use the 
		HashMap<String, PriorityQueue<String>>hashMap = new HashMap<String, PriorityQueue<String>>();
		

		routeMatcher.get("/put", new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				MultiMap map = req.params();
				final String key = map.get("key");
				final String value = map.get("value");
				//You may use the following timestamp for ordering requests
                                final String timestamp = new Timestamp(System.currentTimeMillis() 
                                                                + TimeZone.getTimeZone("EST").getRawOffset()).toString();
                if(!hashMap.containsKey(key)) {
                	PriorityQueue<String>lbq = new PriorityQueue<String>(timestampComparator);
                	lbq.offer(timestamp);
                	hashMap.put(key, lbq);
                }else {	
                	PriorityQueue<String>lbq = hashMap.get(key);
                	lbq.offer(timestamp);
                }
                                
                
				Thread t = new Thread(new Runnable() {
					public void run() {
		
						//TODO: Write code for PUT operation here. 
						//Each PUT operation is handled in a different thread.
						//Highly recommended that you make use of helper functions.
					    PriorityQueue<String>currlbq = hashMap.get(key);
					    synchronized (currlbq) {
						    while(!currlbq.peek().equals(timestamp)) {
					    			try {
										currlbq.wait();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
					
					    		}
						    if(storageType.equals("replication")) {
						    	try {
									KeyValueLib.PUT(dataCenter1, key, value);
									KeyValueLib.PUT(dataCenter2, key, value);
									KeyValueLib.PUT(dataCenter3, key, value);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
						    }else {
						    	try {
						    		System.out.println("cool put");
									KeyValueLib.PUT(hashFunction(key), key, value);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}	
						    }
		                        currlbq.poll();
		                    	currlbq.notifyAll();
						}
				}
				});
				t.start();
				req.response().end(); //Do not remove this
			}
		});

		routeMatcher.get("/get", new Handler<HttpServerRequest>() {
			
			@Override
			public void handle(final HttpServerRequest req) {
		
				MultiMap map = req.params();
				final String key = map.get("key");
				final String loc = map.get("loc");
				//You may use the following timestamp for ordering requests
				final String timestamp = new Timestamp(System.currentTimeMillis() 
								+ TimeZone.getTimeZone("EST").getRawOffset()).toString();
				
				if(!hashMap.containsKey(key)) {
                	PriorityQueue<String>lbq = new PriorityQueue<String>(timestampComparator);
                	lbq.offer(timestamp);
                	hashMap.put(key, lbq);
                }else {	
                	PriorityQueue<String>lbq = hashMap.get(key);
                	lbq.offer(timestamp);
                }
				
				Thread t = new Thread(new Runnable() {
					public void run() {
						//TODO: Write code for GET operation here.
                        //Each GET operation is handled in a different thread.
                        //Highly recommended that you make use of helper functions.
						PriorityQueue<String>currpbq = hashMap.get(key);
				 synchronized (currpbq) {
					    String result ="0";
					    while(!currpbq.peek().equals(timestamp)) {
					    	
								try {
									 currpbq.wait();
								} catch (InterruptedException e) {
									e.printStackTrace();
								}
							}
					    	
						
					    String location = "";
				if(storageType.equals("replication")) {
		                int locationInt = Integer.parseInt(loc);

		                switch (locationInt){
		                    case 1:
		                        location = dataCenter1;
		                        break;
		                    case 2:
		                        location = dataCenter2;
		                        break;
		                    case 3:
		                        location = dataCenter3;
		                        break;
		                }
				}else {
					location = hashFunction(key);
				}
						try {
							result = KeyValueLib.GET(location, key);
						} catch (IOException e) {
							e.printStackTrace();
						}
						
						currpbq.poll();
					    currpbq.notifyAll();
						req.response().end(result); //Default response = 0
				 }
					}
				});
				t.start();
			}
		});

		routeMatcher.get("/storage", new Handler<HttpServerRequest>() {
            @Override
            public void handle(final HttpServerRequest req) {
                    MultiMap map = req.params();
                    storageType = map.get("storage");
                    if(storageType.equals("sharding")) {
                    	storageType = "sharding";
                    }
                    //This endpoint will be used by the auto-grader to set the 
	//consistency type that your key-value store has to support.
                    //You can initialize/re-initialize the required data structures here
                    req.response().end();
            }
    });
		routeMatcher.noMatch(new Handler<HttpServerRequest>() {
			@Override
			public void handle(final HttpServerRequest req) {
				req.response().putHeader("Content-Type", "text/html");
				String response = "Not found.";
				req.response().putHeader("Content-Length",
						String.valueOf(response.length()));
				req.response().end(response);
				req.response().close();
			}
		});
		server.requestHandler(routeMatcher);
		server.listen(8080);
	}
}
