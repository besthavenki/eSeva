package com.elasticsearch.curd;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.reindex.BulkByScrollResponse;
import org.elasticsearch.index.reindex.DeleteByQueryAction;
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

/**
 * Venki!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
    	
    	Settings settings = Settings.builder()
				  .put( "cluster.name", "elasticsearch" )
				  .put( "client.transport.ignore_cluster_name", true )
				  .put( "client.transport.sniff", true )
				  .build();
				
				TransportClient client = new PreBuiltTransportClient( settings ); 
				
		// establishing connection with elastic search		
    	try {
			client.addTransportAddress( new TransportAddress( InetAddress.getByName( "localhost" ), 9300) );
		} catch (UnknownHostException e) {
			System.out.println("connection failed");
			e.printStackTrace();
		}
    	
    	//Preparing Index
		
		/*
		 * try {
		 * 
		 * IndexResponse response = client.prepareIndex("hup", "_doc", "1")
		 * .setSource(jsonBuilder() .startObject() .field("user", "balayya")
		 * .field("postDate", new Date()) .field("message", "trying out Elasticsearch")
		 * .endObject() ) .get();
		 * 
		 * } catch (IOException e) { e.printStackTrace(); }
		 */
		 
    	
    	//Deleting Document
    	try {
    		
    		
    		//DeleteRequest request=new DeleteRequest(".kibana_task_manager", "_doc", "Maps-maps_telemetry");
    		//IndexResponse response1 = (IndexResponse) client.delete(request);
			
    	}catch(Exception e)  {
    		
    		e.printStackTrace();
    	}  
    	
    	//DeleteByQuery processing
		
		 try { 
			 
			 
			 
			 //Delete document by Id
			 //DeleteResponse response = client.prepareDelete("abcairways", "employee", "601").get();
			
			 
			 
			 //Delete document by DeleteByQuery
			 BulkByScrollResponse response =
					  new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
					    .filter(QueryBuilders.matchQuery("firstname", "nikki")) 
					    .source("abcairways")
					    .get();                                             
					long deleted = response.getDeleted();
					System.out.println("count ====="+deleted);
					
					
		  
		 }catch(Exception e) { e.printStackTrace(); }
		 
    }

}
