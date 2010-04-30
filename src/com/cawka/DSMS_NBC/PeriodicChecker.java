package com.cawka.DSMS_NBC;

import org.apache.log4j.Logger;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;

public class PeriodicChecker implements UpdateListener
{
	static private Logger _log=Logger.getLogger( PeriodicChecker.class );
	
	private Configuration _conf;
	private Double _threshold;
	
	public PeriodicChecker( Configuration conf, Double threshold )
	{
		_conf=conf;
		_threshold=threshold;
	}
	
	public void run( String period, String window )
	{
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider( _conf );
		
		String expression;
		
		expression = 
			"INSERT INTO checker (id,total,good) "+
			"SELECT i.ID,count(*),sum( CASE WHEN c.class=i.class THEN 1 ELSE 0 END ) as good "+//,i.class,c.class " +
			"	FROM InputTuple.win:time("+window+") i, " +
					"candidates.win:time("+window+") c " +
			"	WHERE i.ID=c.id";

		epService.getEPAdministrator().createEPL( expression );

//		expression = "select b.id,b.total,b.good,1.0*b.good/b.total as accuracy from checker as b";
		
		expression = 
			"SELECT b.id,b.total,b.good,1.0*b.good/b.total as accuracy " +
			"	FROM pattern [every timer:interval(1 second) -> b=checker]";
		
        EPStatement statement = 
        	epService.getEPAdministrator().createEPL( expression );
			
        statement.addListener( this );
	}
	
	
	//
	public void update( EventBean[] newEvents, EventBean[] oldEvents )
	{
//		_log.debug( Integer.toString(newEvents.length) );
		for( EventBean event : newEvents )
		{
			_log.info( "id:"+event.get("b.id")+", total:"+event.get("b.total")+", good:"+event.get("accuracy") );
		}
	}
}

