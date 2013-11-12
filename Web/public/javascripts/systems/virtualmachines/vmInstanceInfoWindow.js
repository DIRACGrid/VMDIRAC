/*
 * History window
 */

function showInstanceInfoWindow( instanceData )
{
	var items = [];
	var historyLogPanel = new Ext.grid.GridPanel({
		store : new Ext.data.JsonStore({
			url : 'getHistoryForInstanceID',
			root : 'history',
			fields : [ 'Status', 'Load', 'Jobs', 'TransferredFiles', 'TransferredBytes', 'Update' ],
			autoLoad : true,
			baseParams : { instanceID : instanceData[ 'inst_InstanceID' ] },
			vmInstanceData : instanceData
		}),
		columns : [ { header : 'Update time', sortable : true, dataIndex : 'Update' },
		            { header : 'Status', sortable : false, dataIndex : 'Status' },
		            { header : 'Load', sortable : false, dataIndex : 'Load' },
		            { header : 'Jobs', sortable : false, dataIndex : 'Jobs' },
		            { header : 'Files transferred', sortable : false, dataIndex : 'TransferredFiles' },
		            { header : 'Bytes transferred', sortable : false, dataIndex : 'TransferredBytes' },
		],
		viewConfig : {
			forceFit : true,
		},
		title : 'History Log'
	});
	items.push( historyLogPanel );
	var plotsPanel = generatePlotsPanel( historyLogPanel.getStore() );
	items.push( plotsPanel );
	var tabPanel = new Ext.TabPanel({
		activeTab:0,			
		enableTabScroll:true,
	    items: items,
	    region:'center'
	});
	var extendedInfoWindow = new Ext.Window({
	    iconCls : 'icon-grid',
	    closable : true,
	    autoScroll : true,
	    width : 600,
	    height : 350,
	    border : true,
	    collapsible : true,
	    constrain : true,
	    constrainHeader : true,
	    maximizable : true,
	    layout : 'fit',
	    plain : true,
	    shim : false,
	    title : 'Information for instance '+instanceData[ 'inst_UniqueID'] + ' ('+instanceData[ 'img_Name']+')',
	    items : [ tabPanel ]
	  })
	extendedInfoWindow.show();
}

function generatePlotsPanel( store )
{
	var plotValues = [ [ 'Load|Jobs' ], 
	                   [ 'Load|TransferredFiles' ], [ 'Load|TransferredBytes'],
	                   [ 'Load|Transfer Files' ], [ 'Load|Transfer Bytes' ],
	                   [ 'Jobs|TransferredFiles' ] ];
	
	var plotSpace = new Ext.Panel( { 
			region : 'center',
			acPlot : plotValues[0][0],
		});
	
	var plotCombo = new Ext.form.ComboBox({
		store : new Ext.data.SimpleStore({
		    	fields : [ 'plotName' ],
		    	data : plotValues
			}),
		allowBlank:false,
		editable:false,
		mode : 'local',
		displayField : 'plotName',
		typeAhead:true,
		selectOnFocus : true,
		triggerAction : 'all',
		typeAhead : true,
		value : plotValues[0][0],
	});
	
	var plotButton = new Ext.Toolbar.Button( { text : "Generate plot" } );
	
	var statusToolbar = new Ext.Toolbar({ 
			region : 'north', 
			items : [ 'Plot', plotCombo, "->", plotButton ]
  		});
	var plotTab = new Ext.Panel({
			autoScroll : true,
		    margins : '2 0 2 2',
		    cmargins : '2 2 2 2',
		    items : [ statusToolbar, plotSpace ],
		    title : 'Plots',
		    layout : 'border'
	});
	plotFunc = function(){ plotDataForVM( plotSpace, store ) }
	plotCombo.on( 'change', function(){ plotSpace.acPlot = plotCombo.value; });
	plotButton.on( 'click', plotFunc );
	return plotTab;
}

function plotDataForVM( plotSpace, dataStore )
{
	plotSpace.body.dom.innerHTML = "<h1>Generating plot...</h1><h2>(this can take a while)</h2>";
	var dataSources = plotSpace.acPlot.split("|");
	var readerData = dataStore.reader.jsonData.history;
	var encodedData = [];
	var plotParams = {
		chxr : [],
		chxl : [],
		chds : [],
		chls : [],
	};
	for( var i = 0; i < dataSources.length; i++ )
	{
		var fieldData = extractDataForPlot( dataSources[i], readerData );
		encodedData.push( fieldData.data );
		plotParams.chxr.push( ""+(i*2)+",0,"+fieldData.max );
		plotParams.chxl.push( ""+(i*2+1)+":||"+dataSources[i]+"||" );
		plotParams.chds.push( "0,"+fieldData.max );
		plotParams.chls.push( "3" );
	}
	var imgOps = [];
	imgOps.push( "cht=lc" );
	imgOps.push( "chtt=" + dataSources.join( ' vs ' ) + " for " + dataStore.vmInstanceData[ 'inst_UniqueID'] );
	//imgOps.push( "chma=0,0,0,20" );
	imgOps.push( "chxt=y,y,r,r,x" );
	imgOps.push( "chxr=" + plotParams.chxr.join( "|") );
	imgOps.push( "chxl=" + plotParams.chxl.join( "" ) + "4:|"+readerData[0]['Update'] + "|" +readerData[readerData.length - 1]['Update']);
	imgOps.push( "chxs=1,224499,13,0,t|3,fc9906,13,0,t" );
	imgOps.push( "chls=" + plotParams.chls.join( "|") );
	imgOps.push( "chco=224499,fc9906" );
	imgOps.push( "chm=B,76A4FB,0,0,0" );
	imgOps.push( "chs=" + ( plotSpace.getInnerWidth() - 10 ) + "x" + ( plotSpace.getInnerHeight() - 10 ) );
	imgOps.push( "chds=" + plotParams.chds.join( "|") );
	imgOps.push( "chd=e:" + encodedData.join( "," ) );
	var imgSrc = "https://chart.apis.google.com/chart?"+ imgOps.join("&");
	plotSpace.body.dom.innerHTML = "<img src='"+imgSrc+"'/ alt'"+plotSpace.acPlot+"' style:'margin-left:auto;margin-right:auto;'>"
}

//Same as simple encoding, but for extended encoding.
var EXTENDED_MAP=
  'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-.';
var EXTENDED_MAP_LENGTH = EXTENDED_MAP.length;
function extendedEncode(arrVals, maxVal) {
  var chartData = '';

  for(i = 0, len = arrVals.length; i < len; i++) {
    // In case the array vals were translated to strings.
    var numericVal = new Number(arrVals[i]);
    // Scale the value to maxVal.
    var scaledVal = Math.floor(EXTENDED_MAP_LENGTH * 
        EXTENDED_MAP_LENGTH * numericVal / maxVal);

    if(scaledVal > (EXTENDED_MAP_LENGTH * EXTENDED_MAP_LENGTH) - 1) {
      chartData += "..";
    } else if (scaledVal < 0) {
      chartData += '__';
    } else {
      // Calculate first and second digits and add them to the output.
      var quotient = Math.floor(scaledVal / EXTENDED_MAP_LENGTH);
      var remainder = scaledVal - EXTENDED_MAP_LENGTH * quotient;
      chartData += EXTENDED_MAP.charAt(quotient) + EXTENDED_MAP.charAt(remainder);
    }
  }

  return chartData;
}


var simpleEncoding = 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789';

// This function scales the submitted values so that
// maxVal becomes the highest value.
function simpleEncode( valueArray, maxValue ) 
{
  var chartData = [];
  for (var i = 0; i < valueArray.length; i++) {
    var currentValue = valueArray[i];
    if (!isNaN(currentValue) && currentValue >= 0) {
    chartData.push(simpleEncoding.charAt(Math.round((simpleEncoding.length-1) * 
      currentValue / maxValue)));
    }
      else {
      chartData.push('_');
      }
  }
  return chartData.join('');
}


function extractDataForPlot( field, readerData )
{
	var data = [];
	var maxValue = 0;
	var deacum = false;
	if( field.indexOf( 'Transfer ' ) == 0 )
	{
		deacum = true;
		field = field.replace( 'Transfer ', 'Transferred' );
	}
	for( var i = 0; i < readerData.length; i++ )
	{
		var value = readerData[i][ field ];
		data.push( [ readerData[i][ 'Update' ], readerData[i][ field ] ] );
		if( value > maxValue )
			maxValue = value;
	}
	data.sort();
	for( var i = 0; i < data.length; i++ )
	{
		data[i] = data[i][1];
	}
	if( deacum )
	{
		maxValue = 0;
		for( var i = data.length -1; i > 0; i-- )
		{
			data[ i ] = data[ i ] - data[ i - 1 ];
			if( data[i] < 0 )
				data[i] = 0;
			if( data[i] > maxValue )
				maxValue = data[i];
		}
	}
	maxValue = parseInt( maxValue + 1 );
	while( data.length > 200 )
	{
		var reducedData = [];
		for( var i = 0; i< data.length -1; i+=2 )
		{
			reducedData.push( ( data[i]+data[i+1] ) / 2 );
		}
		data = reducedData;
	}
	return { max : maxValue, entries : data.length, data : extendedEncode( data, maxValue ) }
}
