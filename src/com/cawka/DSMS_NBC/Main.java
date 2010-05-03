package com.cawka.DSMS_NBC;

import java.io.File;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.ConfigurationDBRef;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esperio.AdapterInputSource;
import com.espertech.esperio.csv.CSVInputAdapter;
import com.espertech.esperio.csv.CSVInputAdapterSpec;
import com.espertech.esperio.db.config.ConfigurationDBAdapter;

public class Main 
{
	final static Logger _log = Logger.getLogger( Main.class );
	
	
	private final static String _fieldNames[] = { 
		"X_BOX", "Y_BOX", "WIDTH", "HIGH",
		"ONPIX", "X_BAR", "Y_BAR", "X2BAR",
		"Y2BAR", "XYBAR", "X2YBR", "XY2BR",
		"X_EGE", "XEGVY", "Y_EGE", "YEGVX" };

	
	private static Map<String,Object> _typeMap;
	
	private static void init( Configuration conf )
	{
		//////////////// 
        _typeMap = new HashMap<String,Object>( );

        _typeMap.put( "class", String.class );
        for( String field : _fieldNames )
        {
        	_typeMap.put( field, int.class );
        }
        _typeMap.put( "ID", int.class );
        conf.addEventType( "InputTuple", _typeMap );
        
        ConfigurationDBAdapter adapterConfig = new ConfigurationDBAdapter();
        ConfigurationDBRef configDB          = new ConfigurationDBRef( ); 

        // Set properties
        java.util.Properties properties = new java.util.Properties();
        properties.setProperty( "user", "cawka" );
        properties.setProperty( "password", "password" );
        properties.setProperty( "column-change-case", "lowercase" );

        configDB.setDriverManagerConnection( "com.ibm.db2.jcc.DB2Driver", "jdbc:db2://localhost:50001/SAMPLE", properties ); 
        adapterConfig.getJdbcConnections().put( "db2", configDB );
        configDB.setLRUCache( 1000000 );
//        
        conf.addDatabaseReference( "db2", configDB );
	}
	
	
	public static void main( String[] args ) throws InstantiationException, IllegalAccessException, ClassNotFoundException, SQLException
	{
		DOMConfigurator.configure( "logger.xml" );
		
		Configuration conf = new Configuration( );
		conf.configure( new File("configuation.xml") );
		conf.getEngineDefaults().getThreading().setListenerDispatchPreserveOrder( false );
		
		conf.addEventTypeAutoName( Main.class.getPackage().getName() );
		
		init( conf );
		
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider( conf );
		String statement = 
			"CREATE WINDOW trainSet.win:length(1000) as select * from InputTuple";
		epService.getEPAdministrator().createEPL( statement );
		
		statement = 
			"INSERT INTO trainSet SELECT * FROM InputTuple";
		epService.getEPAdministrator().createEPL( statement );
		
		////////// TASK 1. Performing Naive Bayes Classification over a data stream
		
		StreamClassifier classifier=new StreamClassifier( conf );
		classifier.verticalizeTuples( _fieldNames );
		EPStatement stmt=
			classifier.run( _fieldNames.length+1 );

		/////////// TASK 2. Periodically check quality of the classifier
		
		PeriodicChecker checker=new PeriodicChecker( conf, stmt );
		checker.run( "2 seconds", 100, 0.9 ); //every two seconds test last 100 tuples to satisfy threshold 0.7
		
		//////////
		
        _log.info( "started" );
                
        // emulate a continuous input
        AdapterInputSource adapterInputSource = new AdapterInputSource( new File("simulation.csv") );
        
        CSVInputAdapterSpec spec = new CSVInputAdapterSpec( adapterInputSource, "InputTuple" ); 
        spec.setEventsPerSec( 400 ); 
        spec.setLooping( true );
        spec.setUsingEngineThread( true );
        
        (new CSVInputAdapter(epService, spec)).start();

        try 
        {
			Thread.sleep( 10000 );
		} 
        catch( InterruptedException e )
        {
			e.printStackTrace();
		}
        
        _log.info( "stopped" );
        	
	}


}
