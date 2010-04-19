var gPanels = {};

function initVMDashboard(){
  google.load('visualization', '1', {'packages':['piechart','annotatedtimeline']});
  google.setOnLoadCallback( secondInitDashboard );
}

function secondInitDashboard(){
  Ext.onReady(function(){
    renderPage();
    drawDashboardPlots();
  });
}

function renderPage()
{
	gPanels.load = new Ext.Panel ({
		region : 'center',
		html : 'load',
		height : '50%',
		vmTitle : 'Load'
	});

	gPanels.running = new Ext.Panel ({
		region : 'center',
		html : 'running',
		height : '50%',
		vmTitle : 'Running'
	});
	
	gPanels.jobs = new Ext.Panel ({
		region : 'south',
		html : 'jobs',
		vmTitle : 'Jobs'
	});

	gPanels.transfers = new Ext.Panel ({
		region : 'south',
		html : 'transfers',
		vmTitle : 'Transfers'
	});
	
	var rightPanelContainer = new Ext.Panel ({
		region : 'center',
		layout : 'border',
		items : [ gPanels.running, gPanels.transfers ],
	}); 
	
	var leftPanelContainer = new Ext.Panel ({
		region : 'west',
		layout : 'border',
		width : '50%',
		items : [ gPanels.load, gPanels.jobs ],
	}); 
	
	var mainPanels =  [ leftPanelContainer, rightPanelContainer ];
	renderInMainViewport(  mainPanels );
	
	for( var i= 0; i < mainPanels.length; i++ )
	{
		var panel = mainPanels[ i ];
		var height = panel.getInnerHeight();
		var subPanels = panel.items.items;
		var subHeight = parseInt( ( height - 2 )/ subPanels.length );
		for( var j = 0; j < subPanels.length; j ++ )
		{
			subPanels[ j ].setHeight( subHeight );
			if( j > 0 )
			{
				var prevPos = subPanels[j-1].getPosition()
				subPanels[j].setPagePosition( prevPos[0], prevPos[1] + subHeight );
			}
			var divDim = [ subPanels[j].getInnerWidth() - 2, subPanels[j].getInnerHeight() - 20 ];
			var vT = subPanels[j].vmTitle;
			var pHTML = "<p style='text-align:center'>" + vT + "</p><div id='"+vT+"' style='width: "+divDim[0]+"px; height: "+divDim[1]+"px;'></div>"
			subPanels[j].body.dom.innerHTML = pHTML;
		}
	}
}

function drawDashboardPlots()
{
	plotLoad();
}

function plotLoad()
{
	Ext.Ajax.request({
		url : "getGroupedInstanceHistory",
		success : ajaxCBPlotLoad,
		failure : ajaxFailure,
		params : { vars : Ext.util.JSON.encode( [ 'Load' ] ) }
	});
}

function ajaxCBPlotLoad( ajaxResponse, reqArguments )
{
	var retVal = Ext.util.JSON.decode( ajaxResponse.responseText );
	if( ! retVal.OK )
	{
		alert( "Failed to plot history: " + retVal.Message );
		return
	}
	var plotData = retVal.Value;
	
	var dataTable = new google.visualization.DataTable();
	for( var i = 0; i < plotData.fields.length; i++ )
	{
		var field = plotData.fields[i];
		if( field == "Update" )
			dataTable.addColumn( 'date', 'Date' );
		else
			dataTable.addColumn( 'number', field )
	}
	var rows = [];
	var utcOffset = ( new Date() ).getTimezoneOffset() * 60000;
	for( var i = 0; i < plotData.data.length; i++ )
	{
		var record = plotData.data[i];
		var row = [];
		for( var j = 0; j < record.length; j++ )
		{	
			if( plotData.fields[j] == 'Update' )
			{
				var s = ( record[j] * 1000 ) - utcOffset;

				var d = new Date( s );
				console.log( d );
				row.push( d );
			}
			else
			{
				row.push( record[j] );
			}
		}
		rows.push( row );
	}
	dataTable.addRows( rows );
	var chart = new google.visualization.AnnotatedTimeLine(document.getElementById(gPanels.load.vmTitle));
    chart.draw(dataTable, {
    	displayAnnotations: true,
    	//scaleColumns : [0,1,2],
    	scaleType : 'allmaximized',
    	fill : 20,
    	displayRangeSelector : false,
    	thickness : 3,
    	displayZoomButtons : false,
    	displayGrid : false,
    	dateFormat : 'y-MM-dd HH:mm (v)'
    	});
}


/*
 * OLD
 * 
 */

function oldRenderPage()
{
    gPiePanel = new Ext.Panel({ 
    	html : 'Generating pie chart...',
    	width : 400,
		tbar : new Ext.Toolbar({
			items : [ "dummy" ]
		}),
    });
	var gridPanel =new Ext.Panel({ 
		html : 'jar',
	});
	gHistPanel = new Ext.Panel({  
		colspan : 2,
		height : 450,
		tbar : new Ext.Toolbar({
			items : [ "dummy"]
		}),
	});
	gMainPanel = new Ext.Panel({
		layout : 'table',
		layoutConfig : { columns : 2 },
		defaults: {
	        bodyStyle: { padding : '0px 0px 0px 0px',
						 margin : '0px 0px 0px 0px',
						 'border-spacing' : '0px 0px 0px 0px',
			}
	    },
		region : 'center',
		cls : 'allPanel',
		items : [ gPiePanel, gridPanel, gHistPanel ]
	});
	renderInMainViewport( [ gMainPanel ] );
}

function oldDrawDashboardPlots()
{
	plotStatusCounters();
	plotHistory();
}

function plotStatusCounters()
{
	Ext.Ajax.request({
		url : "getInstanceStatusCounters",
		success : ajaxCBPlotStatusCounters,
		failure : ajaxFailure,
	});
}

function ajaxCBPlotStatusCounters( ajaxResponse, reqArguments )
{
	var retVal = Ext.util.JSON.decode( ajaxResponse.responseText );
	if( ! retVal.OK )
	{
		alert( "Failed to plot status counters: " + retVal.Message );
		return
	}
	var plotData = retVal.Value;
	gPiePanel.body.dom.innerHTML = "<div id='piePlotSpace'></div>"
	var dataTable = new google.visualization.DataTable();
	dataTable.addColumn('string', 'Status');
	dataTable.addColumn('number', 'Instances');
	var rows = [];
	for( k in plotData )
		rows.push( [ k, plotData[k] ] );
	dataTable.addRows( rows );

    // Instantiate and draw our chart, passing in some options.
    var chart = new google.visualization.PieChart(document.getElementById('piePlotSpace'));
    chart.draw( dataTable, 
    		    { 
    			  width: 400,          
    			  height: 240, 
    	          is3D: true, 
    	          title: 'Instances by Status'
    		    }
    );
}

function plotHistory()
{
	Ext.Ajax.request({
		url : "getGroupedInstanceHistory",
		success : ajaxCBPlotHistory,
		failure : ajaxFailure,
	});
}

function ajaxCBPlotHistory( ajaxResponse, reqArguments )
{
	var retVal = Ext.util.JSON.decode( ajaxResponse.responseText );
	if( ! retVal.OK )
	{
		alert( "Failed to plot history: " + retVal.Message );
		return
	}
	var plotData = retVal.Value;
	var height = gHistPanel.getInnerHeight();
	if( height < 400 )
		height = 400;
	var width  = gHistPanel.getInnerWidth();
	gHistPanel.body.dom.innerHTML = "<div id='historyPlotSpace' style='width: "+width+"px; height: "+height+"px;'></div>"
	
	var dataTable = new google.visualization.DataTable();
	//for( var i = 0; i < plotData.fields.length; i++ )
	for( var i = 0; i < 2; i++ )
	{
		var field = plotData.fields[i];
		if( field == "Update" )
			dataTable.addColumn( 'date', 'Date' );
		else
			dataTable.addColumn( 'number', field )
	}
	var rows = [];
	for( var i = 0; i < plotData.data.length; i++ )
	{
		var record = plotData.data[i];
		var row = [];
		//for( var j = 0; j < record.length; j++ )
		for( var j = 0; j < 2; j++ )
		{	
			if( plotData.fields[j] == 'Update' )
			{
				var s = record[j].split( " " );
				var date = s[0].split("-");
				var time = s[1].split(":");
				var d = new Date( parseInt( date[0] ), parseInt( date[1] ), parseInt( date[2] ),
							      parseInt( time[0] ), parseInt( time[1] ), parseInt( time[2] ) );
				row.push( d );
			}
			else
			{
				row.push( record[j] );
			}
		}
		rows.push( row );
	}
	dataTable.addRows( rows );
	var chart = new google.visualization.AnnotatedTimeLine(document.getElementById('historyPlotSpace'));
    chart.draw(dataTable, {
    	displayAnnotations: true,
    	//scaleColumns : [0,1,2],
    	scaleType : 'allmaximized',
    	fill : 20,
    	displayRangeSelector : false,
    	thickness : 4,
    	displayZoomButtons : false,
    	dateFormat : 'y-MM-dd HH:mm'
    	});
}

function ajaxFailure( ajaxResponse, reqArguments )
{
	alert( "Error in AJAX request : " + ajaxResponse.responseText );
}