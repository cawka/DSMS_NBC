package com.cawka.DSMS_NBC;

import java.util.HashMap;

import org.apache.log4j.Logger;

import com.espertech.esper.epl.agg.AggregationSupport;

@SuppressWarnings("unchecked")
public class Predict extends AggregationSupport 
{
	static private Logger _log=Logger.getLogger( Predict.class );

	private HashMap<String,pair> _map=new HashMap<String,pair>( );
	private int _max_parameters;
	
	
	public void validate( Class childNodeType ) 
	{
		if( !childNodeType.getClass().equals(Object[].class) )
			throw new IllegalArgumentException( "'predict' aggregate requires two paramters: String and Double" );
		
		//assume we did the validation
	}
	
	public void validateMultiParameter( Class[] parameterType, boolean[] isConstantValue, Object[] constantValue )
	{
		if( parameterType.length!=3 )
			throw new IllegalArgumentException( "You should supply 3 parameters to 'predict' aggregate: (class,probability,num_parameters)");
		
		if( parameterType[0]!=String.class )
			throw new IllegalArgumentException( "First parameter for 'predict' aggregate should be String (class name)");
		
		if( parameterType[1]!=Double.class )
			throw new IllegalArgumentException( "Second parameter for 'predict' aggregate should be Double (logarithm of the probability)" );
			
		if( parameterType[2]!=Integer.class || !isConstantValue[2] )
			throw new IllegalArgumentException( "Third parameter for 'predict' aggregate should be a Integer constant (number of parameters in NBC model)" );
	}
	

	public void clear( ) 
	{
		_log.debug( "clear" );
		_map=new HashMap<String,pair>( );
	}

	public void enter( Object value ) 
	{
		Object params[]=(Object[])value;
		if( _max_parameters==0 ) _max_parameters=(Integer)params[2];

		pair sum=_map.get( (String)params[0] );
		if( sum!=null )
			sum.addValue( (Double)params[1] );
		else
			_map.put( (String)params[0], new pair((Double)params[1]) );
	}

	public void leave( Object value ) 
	{
		Object params[]=(Object[])value;
		
		pair sum=_map.get( (String)params[0] );
		if( sum==null ) return;

		sum.removeValue( (Double)params[1] );
		if( sum.empty() ) _map.remove( (String)params[0] );
	}
	
	public Object getValue() 
	{
//		_log.debug( "getValue" );
		
		String ret=null;
		Double max=-Double.MAX_VALUE;
		for( String key : _map.keySet() )
		{
			pair sum=_map.get( key );
			if( sum.nonZero() && sum.getValue().compareTo(max)>0 )
			{
				ret=key;
				max=sum.getValue();
			}
		}
		return ret;
	}

	public Class getValueType () 
	{
		return String.class;
	}

	
	
	private class pair
	{
		Double _val;
		int    _counter;
		
		public pair( Double val )
		{
			_counter=1;
			_val=val;
		}
		
		public Double getValue( ) { return _val; }
		public void   addValue( Double val ) { _val+=val; _counter++; }
		public void   removeValue( Double val ) { _val-=val; _counter--; }
		
		public boolean nonZero( ) 
		{ 
			return _counter==_max_parameters; 
		}
		
		public boolean empty( )
		{
			return _counter==0;
		}
	}

}
