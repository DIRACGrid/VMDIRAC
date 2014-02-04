var gPanels = {};

function initVMDashboard(){
  google.load('visualization', '1', {'packages':['piechart','annotatedtimeline']});
  google.setOnLoadCallback( secondInitDashboard );
}

function secondInitDashboard(){
  Ext.onReady(function(){
    renderPage();
    drawDashboardPlots();
    setInterval( "drawDashboardPlots()", 3600 * 1000 ); //redraw every hour
  });
}

function renderPage()
{

	gPanels.load = new plotSpace ({
			region : 'center',
			html : 'load',
			height : '50%',
		},
		{ defaultPlot : 'load' }
	);

	gPanels.running = new plotSpace ({
			region : 'center',
			html : 'running',
			height : '50%',
		},
		{ defaultPlot : 'running' }
	);
	
	gPanels.jobs = new plotSpace ({
			region : 'south',
			html : 'jobs',
		},
		{ defaultPlot : 'jobs' }
	);

	gPanels.transfers = new plotSpace ({
			region : 'south',
			html : 'transfers',
		},
		{ defaultPlot : 'transferbytes' }
	);
	
	var rightPanelContainer = new Ext.Panel ({
		region : 'center',
		layout : 'border',
		items : [ gPanels.running.createPanel(), gPanels.transfers.createPanel() ],
	}); 
	
	var leftPanelContainer = new Ext.Panel ({
		region : 'west',
		layout : 'border',
		width : '50%',
		items : [ gPanels.load.createPanel(), gPanels.jobs.createPanel() ],
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
		}
	}
}

function drawDashboardPlots()
{
	for( name in gPanels )
	{
		gPanels[ name ].drawPlot();
	}
}

function plotSpace( panelConfig, spaceConfig )
{
	this.panelConfig = panelConfig;
	this.spaceConfig = spaceConfig;
	
	if( spaceConfig.timespan )
		this.plotTimespan = spaceConfig.timespan;
	else
		this.plotTimespan = 86400;
	
	if( spaceConfig.defaultPlot )
		this.plotName = spaceConfig.defaultPlot;
	else
		this.plotName = 'Load';
	
	this.generatePlotSpaceToolbar = function()
	{
		var timeStore = new Ext.data.SimpleStore({
			fields:[ 'timespanValue', 'plotTimespan'],
			data:[ [ 86400, 'Last day' ], [ 86400*7, 'Last week' ],
			       [ 86400*30, 'Last month' ], [ 0, 'All history' ] ]
		});
		var timeCombo = new Ext.form.ComboBox({
			allowBlank:false,
			displayField:'plotName',
			editable:false,
			mode:'local',
			valueField:'timespanValue',
			displayField : 'plotTimespan',
			selectOnFocus:true,
			store:timeStore,
			triggerAction:'all',
			typeAhead:true,
			width : 100,
			value: timeStore.getAt(0).get( 'plotTimespan' )
		});
		timeCombo.on( 'collapse', 
				function( combo )
				{
					this.plotTimespan = combo.getValue();
					this.drawPlot();
				},
				this
		);
		
		var plotStore = new Ext.data.SimpleStore({
			fields:[ 'plotName', 'plotText' ],
			data:[ [ 'load', 'Average load' ], [ 'running', 'Running VMs' ], [ 'runningbyendpoint', 'Run. VMs by EndPoint' ],
			       [ 'runningbyrunningpod', 'Run. VMs by RunningPod' ], [ 'runningbyimage', 'Run. VMs by Image' ],
			       [ 'jobs', 'Started Jobs' ], [ 'transferbytes', 'Transferred Data' ],
			       [ 'transferfiles', 'Transferred Files' ] ]
		});
		var sP = 0;
		for( var i = 0; i < plotStore.getCount(); i++ )
		{
			if( plotStore.getAt( i ).get( 'plotName' ) == this.plotName )
			{
				sP = i;
				break;
			}
		}
		var plotCombo = new Ext.form.ComboBox({
			allowBlank:false,
			displayField:'plotName',
			editable:false,
			mode:'local',
			valueField:'plotName',
			displayField : 'plotText',
			selectOnFocus:true,
			store:plotStore,
			triggerAction:'all',
			typeAhead:true,
			width : 150,
			value: plotStore.getAt(sP).get( 'plotText' )
		});
		plotCombo.on( 'collapse', 
				function( combo )
				{
					this.plotName = combo.getValue();
					this.drawPlot();
				},
				this
		);
		
		return new Ext.Toolbar({
			items : [ 'Plot ', plotCombo, ' ', "Timespan ", timeCombo ]
		});
	}
	
	this.createPanel = function()
	{
		this.tbar = this.generatePlotSpaceToolbar()
		this.panelConfig.tbar = this.tbar;
		this.panel = new Ext.Panel( this.panelConfig );
		return this.panel;
	}
	
	this.drawPlot = function()
	{
		var divDim = [ this.panel.getInnerWidth() - 2, this.panel.getInnerHeight() - 2 ];
		var panelPos = this.panel.getPosition()
		this.divID = ""+panelPos[0]+"_"+panelPos[1];
			
		var pHTML = "<div id='"+this.divID+"' style='width: "+divDim[0]+"px; height: "+divDim[1]+"px;'></div>"
		this.panel.body.dom.innerHTML = pHTML;

		switch( this.plotName )
		{
			case 'load':
				this.requestLoadPlot();
				break;
			case 'running':
				this.requestRunningPlot();
				break;
			case 'runningbyendpoint':
				this.requestRunningByEndPointPlot();
				break;				
			case 'runningbyrunningpod':
				this.requestRunningByRunningPodPlot();
				break;				
			case 'runningbyimage':
				this.requestRunningByImagePlot();
				break;				
			case 'jobs':
				this.requestJobsPlot();
				break;
			case 'transferbytes':
				this.requestTransferBytesPlot();
				break;
			case 'transferfiles':
				this.requestTransferFilesPlot();
				break;
		}
	}
	
	this.requestTransferFilesPlot = function()
	{
		Ext.Ajax.request({
			url : "getHistoryValues",
			success : this.plotHistoryValue,
			failure : ajaxFailure,
			scope : this,
			params : { 
				vars : Ext.util.JSON.encode( [ 'TransferredFiles' ] ),
				timespan : this.plotTimespan
			}
		});
	}
	
	this.requestTransferBytesPlot = function()
	{
		Ext.Ajax.request({
			url : "getHistoryValues",
			success : this.plotHistoryValue,
			failure : ajaxFailure,
			scope : this,
			params : { 
				vars : Ext.util.JSON.encode( [ 'TransferredBytes' ] ),
				timespan : this.plotTimespan
			}
		});
	}

	this.requestJobsPlot = function()
	{
		Ext.Ajax.request({
			url : "getHistoryValues",
			success : this.plotHistoryValue,
			failure : ajaxFailure,
			scope : this,
			params : { 
				vars : Ext.util.JSON.encode( [ 'Jobs' ] ),
				timespan : this.plotTimespan
			}
		});
	}

	this.requestLoadPlot = function()
	{
		Ext.Ajax.request({
			url : "getHistoryValues",
			success : this.plotHistoryValue,
			failure : ajaxFailure,
			scope : this,
			params : { 
				vars : Ext.util.JSON.encode( [ 'Load' ] ),
				timespan : this.plotTimespan
			}
		});
	}

	this.plotHistoryValue = function( ajaxResponse, reqArguments )
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
			switch( field )
			{
				case 'Update': 
					dataTable.addColumn( 'date', 'Date' );
					break;
				case 'Jobs':
					dataTable.addColumn( 'number', 'Started jobs' );
					break;
				case 'TransferredFiles':
					dataTable.addColumn( 'number', 'Files transferred' );
					break;
				case 'TransferredBytes':
					dataTable.addColumn( 'number', 'Data transferred (GiB)' );
					break;
				default:
					dataTable.addColumn( 'number', field );
			}
		}
		var rows = [];
		var utcOffset = ( new Date() ).getTimezoneOffset() * 60000;
		for( var i = 0; i < plotData.data.length; i++ )
		{
			var record = plotData.data[i];
			var row = [];
			for( var j = 0; j < record.length; j++ )
			{	
				switch( plotData.fields[j] )
				{
					case 'Update':
						var s = ( record[j] * 1000 ) - utcOffset;
						var d = new Date( s );
						row.push( d );
						break;
					case 'TransferredBytes':
						row.push( record[j] / 1073741824.0 );
						break;
					default:
						row.push( record[j] );
				}
			}
			rows.push( row );
		}
		dataTable.addRows( rows );
		var chart = new google.visualization.AnnotatedTimeLine(document.getElementById(this.divID));
		var sC = [];
		for( var i= 0; i < plotData.fields.length - i; i++ )
		{
			if( i > 2 )
				continue;
			sC.push( i );
		}
		var colors=[]
	    for( var i = 0; i < plotData.fields.length; i++ )
		{
			switch( plotData.fields[i] )
			{
				case 'Load':
					colors.push( '4684ee' );
					break;
				case 'TransferredBytes':
					colors.push( 'c9710d' );
					break;
				case 'TransferredFiles':
					colors.push( '0ab58c' );
					break;
				case 'Jobs':
					colors.push( 'b00c12' );
					break;
			}
		}
		var chartConfig = {
		    	displayAnnotations: true,
		    	//scaleColumns : [0,1,2],
		    	scaleColumns : sC,
		    	scaleType : 'allmaximized',
		    	fill : 20,
		    	displayRangeSelector : false,
		    	thickness : 3,
		    	displayZoomButtons : false,
		    	displayGrid : false,
		    	dateFormat : 'y-MM-dd HH:mm (v)',
		    	colors : colors,
		    	min : 0
		    };
		if( this.plotTimespan == 0 )
			chartConfig.displayRangeSelector = true;
		
	    chart.draw( dataTable, chartConfig );
	}
	
	this.requestRunningPlot = function()
	{
		Ext.Ajax.request({
			url : "getRunningInstancesHistory",
			success : this.plotRunning,
			failure : ajaxFailure,
			scope : this,
			params : { 
				bucketSize : 900,
				timespan : this.plotTimespan
			}
		});
	}
	
	this.requestRunningByEndPointPlot = function()
	{
		Ext.Ajax.request({
			url : "getRunningInstancesBEPHistory",
			success : this.plotRunningByFields,
			failure : ajaxFailure,
			scope : this,
			params : { 
				bucketSize : 900,
				timespan : this.plotTimespan
			}
		});
	}

	this.requestRunningByRunningPodPlot = function()
	{
		Ext.Ajax.request({
			url : "getRunningInstancesByRunningPodHistory",
			success : this.plotRunningByFields,
			failure : ajaxFailure,
			scope : this,
			params : { 
				bucketSize : 900,
				timespan : this.plotTimespan
			}
		});
	}

	this.requestRunningByImagePlot = function()
	{
		Ext.Ajax.request({
			url : "getRunningInstancesByImageHistory",
			success : this.plotRunningByFields,
			failure : ajaxFailure,
			scope : this,
			params : { 
				bucketSize : 900,
				timespan : this.plotTimespan
			}
		});
	}

	this.plotRunningByFields = function( ajaxResponse, reqArguments )
	{
		var retVal = Ext.util.JSON.decode( ajaxResponse.responseText );
		if( ! retVal.OK )
		{
			alert( "Failed to plot running: " + retVal.Message );
			return
		}
		var plotData = retVal.Value;
		
		var matrix = [];
		for( var i = 0; i < plotData.length; i++ )
		{
			var record = plotData[i];
			var dev = 0;
			for( var n = 0; n < matrix.length; n++ )
			{	
				if ( matrix[n] == record[1]){
					dev = 1;
				}
			}
			if ( dev == 0 ){
				matrix.push(record[1]);
			}
		}		
		
		var dates = [];
		for( var i = 0; i < plotData.length; i++ )
		{
			var record = plotData[i];
			var dev = 0;
			for( var n = 0; n < dates.length; n++ )
			{	
				if ( dates[n] == record[0]){
					dev = 1;
				}
			}
			if ( dev == 0 ){
				dates.push(record[0]);
			}
		}			
		var data = new google.visualization.DataTable();
		  data.addColumn('date', 'Date');
		  for(var i = 0; i < matrix.length;i++)
			{
				data.addColumn( 'number', matrix[i] );	
			}
		var utcOffset = ( new Date() ).getTimezoneOffset() * 60000;
		
		for(var i = 0; i < dates.length;i++)
		{
			var row = [];
			var field = new Date( ( dates[i] * 1000 ) - utcOffset );
			row.push(field);			
			var matrix_times_range = [];
			var matrix_times_large = 0;
			for(var j = 0; j < plotData.length;j++)
			{				
				row_time = [];
				var record = plotData[j];
				if (dates[i] == record[0])
				{
					row_time.push(record[1]);
					row_time.push(record[2]);
					matrix_times_large = matrix_times_large + 1;
					matrix_times_range.push(row_time);					
				}
			}
			var dev = 0;
			for(var n=0; n < matrix.length;n++)
			{
				dev = 0;
				for (var m=0; m < matrix_times_large;m++){
					if ( matrix[n] == matrix_times_range[m][0] ){
						row.push(matrix_times_range[m][1]);
						dev=1;
					}
				}
				if ( dev == 0 ){
					row.push(0);
				}
			}	
			data.addRow( row );
		}
		
		var chart = new google.visualization.AnnotatedTimeLine(document.getElementById(this.divID));
		var chartConfig = {
		    	/*displayAnnotations: true,
		    	scaleType : 'allmaximized',*/
		    	fill : 20,
		    	displayRangeSelector : false,
		    	thickness : 3,
		    	displayZoomButtons : false,
		    	displayGrid : false,
		    	dateFormat : 'y-MM-dd HH:mm (v)',
		    	colors : [ '3fa900' ],
		    	min : 0
		    };
		if( this.plotTimespan == 0 )
			chartConfig.displayRangeSelector = true;
		if (dates.length == 0){
			var dataTable = new google.visualization.DataTable();
			dataTable.addColumn( 'date', 'Date' );
			dataTable.addColumn( 'number', 'No VMs' );			
			var rows = [];
			dataTable.addRows( rows );			
		    chart.draw( dataTable, chartConfig );
		} else {
			chart.draw( data, chartConfig );
		}
	}		

	this.plotRunning = function( ajaxResponse, reqArguments )
	{
		var retVal = Ext.util.JSON.decode( ajaxResponse.responseText );
		if( ! retVal.OK )
		{
			alert( "Failed to plot running: " + retVal.Message );
			return
		}
		var plotData = retVal.Value;
		
		var dataTable = new google.visualization.DataTable();
		dataTable.addColumn( 'date', 'Date' );
		dataTable.addColumn( 'number', 'Running VMs' );
		
		var rows = [];
		var utcOffset = ( new Date() ).getTimezoneOffset() * 60000;
		for( var i = 0; i < plotData.length; i++ )
		{
			var record = plotData[i];
			var row = [ new Date( ( record[0] * 1000 ) - utcOffset ), record[1] ];
			rows.push( row );
		}
		dataTable.addRows( rows );
		
		var chart = new google.visualization.AnnotatedTimeLine(document.getElementById(this.divID));
		
		var chartConfig = {
		    	displayAnnotations: true,
		    	scaleType : 'allmaximized',
		    	fill : 20,
		    	displayRangeSelector : false,
		    	thickness : 3,
		    	displayZoomButtons : false,
		    	displayGrid : false,
		    	dateFormat : 'y-MM-dd HH:mm (v)',
		    	colors : [ '3fa900' ],
		    	min : 0
		    };
		if( this.plotTimespan == 0 )
			chartConfig.displayRangeSelector = true;
		
	    chart.draw( dataTable, chartConfig );
	}
}

function ajaxFailure( ajaxResponse, reqArguments )
{
	alert( "Error in AJAX request : " + ajaxResponse.responseText );
}
