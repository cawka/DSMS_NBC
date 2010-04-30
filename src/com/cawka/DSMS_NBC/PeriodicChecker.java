package com.cawka.DSMS_NBC;

import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPOnDemandQueryResult;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class PeriodicChecker implements UpdateListener
{
	static private Logger _log=Logger.getLogger( PeriodicChecker.class );
	
	private Configuration _conf;
	private EPStatement _candidates;
	
	public PeriodicChecker( Configuration conf, EPStatement candidates )
	{
		_conf=conf;
		_candidates=candidates;
	}
	
	public void run( String period, Integer win_length, Double threshold )
	{
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider( _conf );
		
		String expression;
		
		expression = 
			"INSERT INTO checker (id,total,good) "+
			"SELECT i.ID,count(*),sum( CASE WHEN c.class=i.class THEN 1 ELSE 0 END ) as good  "+
			"	FROM InputTuple.win:length("+win_length+") i, " +
					"candidates.win:length("+win_length+") c " +
			"	WHERE i.ID=c.id";

		epService.getEPAdministrator().createEPL( expression );

//		expression = "select b.id,b.total,b.good,1.0*b.good/b.total as accuracy from checker as b";
		
        expression =
        	"INSERT INTO accuracies "+
        	"SELECT min(1.0*b.good/b.total) as accuracy " +
        	"	FROM checker.win:length_batch(100) b ";

    	epService.getEPAdministrator().createEPL( expression );

    	expression = 
			"SELECT b.accuracy as accuracy " +
			"	FROM pattern [every timer:interval("+period+") -> b=accuracies] " +
			"	WHERE b.accuracy<"+threshold;
        
        
        EPStatement statement = 
        	epService.getEPAdministrator().createEPL( expression );
        
        statement.addListener( this );
	}
	
	
	//
	public void update( EventBean[] newEvents, EventBean[] oldEvents )
	{
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider( _conf );

		String expression = "SELECT * FROM trainSet";
		EPOnDemandQueryResult result=epService.getEPRuntime().executeQuery( expression );
		_log.debug( result.getArray().length );
		for( EventBean row : result.getArray() )
		{
			// TODO:
			// JDBC connection to database, updating training set, and requesting to rebuild NBC
		}

		epService.getEPAdministrator().destroyAllStatements( );
		
		// TODO:
		// rebuild and restart all statements
	}
}
