var gMainPanel = false;
var gPiePanel = false
var gHistPanel =  false;

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
	
    //google.setOnLoadCallback( drawDashboardPlots );

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

function drawDashboardPlots()
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
	for( var i = 0; i < plotData.fields.length; i++ )
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
		for( var j = 0; j < record.length; j++ )
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
    	scaleColumns : [0,1,2],
    	scaleType : 'allmaximized',
    	displayZoomButtons : false,
    	});
}

function ajaxFailure( ajaxResponse, reqArguments )
{
	alert( "Error in AJAX request : " + ajaxResponse.responseText );
}