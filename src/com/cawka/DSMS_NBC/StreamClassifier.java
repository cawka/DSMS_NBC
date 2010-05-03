package com.cawka.DSMS_NBC;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;

public class StreamClassifier 
{
	Configuration _conf;
	
	public StreamClassifier( Configuration conf )
	{
		_conf=conf;
	}
	
	// create and fill 'verticalTuples' from 'InputTuple'
	public void verticalizeTuples( String fieldNames[] )
	{
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider( _conf );
		
		String expression=
			"INSERT INTO verticalTuples (id,num,value) "+
			"SELECT ID,-1,0-1 "+ //to add probability of the class
			"	FROM InputTuple";
		
		epService.getEPAdministrator().createEPL( expression );
		
		for( int i=0; i<fieldNames.length; i++ )
		{
			String field=fieldNames[i];
			
			expression=
				"INSERT INTO verticalTuples (id,num,value) "+
				"SELECT ID,"+(i+1)+","+field+
				"	FROM InputTuple";
			
			epService.getEPAdministrator().createEPL( expression );
		}		
	}
	
	public EPStatement run( int max_parameters )
	{
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider( _conf );
		
        String expression = 
        	"INSERT INTO candidates (id,class) "+
        	"SELECT id,predict(n.CLASS,PROB,"+Integer.toString(max_parameters)+") as class "+
        	"FROM verticalTuples.win:length("+Integer.toString(max_parameters)+") as i "+
        	"	JOIN sql:db2 ['select CLASS,NUM,VALUE,PROB from NBC'] as n " +
        	"		ON n.NUM=i.num AND n.VALUE=i.value " +
        	"	GROUP BY id" +
        	"	HAVING predict(n.CLASS,PROB,17) IS NOT NULL"
        	;
		return epService.getEPAdministrator().createEPL( expression );		
	}
}

