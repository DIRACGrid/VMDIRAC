var gMainGrid = false;
var gVMMenu = false;
var auth_response = false;

function initVMBrowser(){
  auth_response = '';
  checkVmWebOperation();
  Ext.onReady(function(){
    renderPage();
  });
}

function renderPage()
{
  gMainGrid = generateBrowseGrid();	
  renderInMainViewport( [gMainGrid] );
}

function generateBrowseGrid( config )
{
	if( config == undefined )
		config = {};
	if( config.vmFilter == undefined )
		config.vmFilter = {}
	var reader = new Ext.data.JsonReader({
		root : 'instances',
		totalProperty : 'numRecords',
		id : 'inst_InstanceID',
		fields : [ 'inst_RunningPod', 'inst_Name', 'img_CloudEndpoints', 'inst_Endpoint', 'inst_ErrorMessage', 'inst_InstanceID', 
                           'inst_Status', 'inst_UniqueID', 
                           'img_VMImageID', 'img_Name', 'inst_VMImageID', 'inst_PublicIP', 'inst_LastUpdate',
		           'inst_Load', 'inst_Uptime', 'inst_Jobs']
    });

	var store = new Ext.data.Store({
		reader: reader,
		url : "getInstancesList",
		autoLoad : true,
		sortInfo: { field: 'inst_LastUpdate', direction: 'DESC' },
		listeners : { 
			beforeload : cbStoreBeforeLoad		
	    },
	    vmCond : config.vmFilter
	});

	if( auth_response == "Auth" )
		aux_tbar= [
   				{ handler:function(){ toggleAll(true) }, text:'Select all', width:150, tooltip:'Click to select all rows' },
   				{ handler:function(){ toggleAll(false) }, text:'Select none', width:150, tooltip:'Click to select all rows' },
   				'->',
  	    			{ handler:function(){ cbStopSelected() }, text:'Stop', width:150, tooltip:'Click to stop all selected VMs' },
      			  ];
	else
		aux_tbar= [
   				{ handler:function(){ toggleAll(true) }, text:'Select all', width:150, tooltip:'Click to select all rows' },
   				{ handler:function(){ toggleAll(false) }, text:'Select none', width:150, tooltip:'Click to select all rows' },
      			  ];
	
	gMainGrid = new Ext.grid.GridPanel( {
		store : store,
		/*view: new Ext.grid.GroupingView({
			groupTextTpl: '{text} ({[values.rs.length]} {[values.rs.length > 1 ? "Items" : "Item"]})',
			emptyText: 'No data',
			startCollapsed : false,
		}),*/
		columns: [
		    { id : 'check', header : '', width : 30, dataIndex: 'inst_InstanceID', renderer : renderSelect },
	            { header: "Image", width: 120, sortable: true, dataIndex: 'img_Name'},
	            { header: "RunningPod", width: 120, sortable: true, dataIndex: 'inst_RunningPod'},
	            { header: "EndPoint", width: 100, sortable: true, dataIndex: 'img_CloudEndpoints'},
	            { header: "Status", width: 100, sortable: true, dataIndex: 'inst_Status'},
	            { header: "Endpoint VM ID", width: 220, sortable: true, dataIndex: 'inst_UniqueID'},
	            { header: "IP", width: 100, sortable: true, dataIndex: 'inst_PublicIP'},
	            { header: "Load", width: 50, sortable: true, dataIndex: 'inst_Load', renderer : renderLoad },
	            { header: "Uptime", width: 75, sortable: true, dataIndex: 'inst_Uptime', renderer : renderUptime },
	            { header: "Jobs", width: 50, sortable: true, dataIndex: 'inst_Jobs' },
	            { header: "Last Update (UTC)", width: 125, sortable: true, dataIndex: 'inst_LastUpdate' },
	            { header: "Error", width: 350, sortable: true, dataIndex: 'inst_ErrorMessage'},
                ],
	        region : 'center',
		tbar : aux_tbar,
      		bbar: new Ext.PagingToolbar({
			pageSize: 50,
			store: store,
			displayInfo: true,
			displayMsg: 'Displaying entries {0} - {1} of {2}',
			emptyMsg: "No entries to display",
			items:[ '-',
			        'Items displaying per page: ', createNumItemsSelector(),
			        '-',
			        'Show VMs in status: ', createStatusSelector() ],
      		}),
      		listeners : { sortchange : cbMainGridSortChange },
	} );
	if( config.title )
		gMainGrid.setTile( config.title );
	
	gVMMenu = new Ext.menu.Menu({
	   	id : 'OptionContextualMenu',
	   	items : [ { text : 'Show information', listeners : { click : cbShowVMHistory }
  				 },
   			  ]
	   })
	gMainGrid.on( 'cellcontextmenu', cbShowContextMenu );
	
	return gMainGrid;
}

function renderSelect( value, metadata, record, rowIndex, colIndex, store )
{
	return '<input id="' + record.id + '" type="checkbox"/>';
}

function renderUptime( value, metadata, record, rowIndex, colIndex, store )
{
	var hour = parseInt( value / 3600 );
	var min = parseInt( ( value % 3600  ) / 60 );
	var sec = parseInt( value % 60 );
	if( min < 10 )
		min = "0"+min;
	if( sec < 10 )
		sec = "0"+sec;
	return ""+hour+":"+min+":"+sec;
}

function renderLoad( value, metadata, record, rowIndex, colIndex, store )
{
	return value.toFixed(2);
}

function toggleAll( select )
{
	var chkbox = document.getElementsByTagName('input');
	for (var i = 0; i < chkbox.length; i++)
	{
		if( chkbox[i].type == 'checkbox' )
		{
			chkbox[i].checked = select;
		}
	}
}

function getSelectedCheckboxes()
{
	var items = [];
	var inputs = document.getElementsByTagName('input');
	for (var i = 0; i < inputs.length; i++)
	{
		if( inputs[i].checked )
		{
        items.push( inputs[i].id );
      }
   }
   return items;
}

function cbStoreBeforeLoad( store, params )
{
	var sortState = store.getSortState()
	var bb = gMainGrid.getBottomToolbar();
	store.baseParams = { 'sortField' : sortState.field,
					     'sortDirection' : sortState.direction,
						 'limit' : bb.pageSize,
					   };
	if( store.vmCond != undefined )
	{
		store.baseParams[ 'cond' ] = Ext.util.JSON.encode( store.vmCond );
	}
	if( bb.statusSelector && bb.statusSelector != "All" )
		store.baseParams.statusSelector = bb.statusSelector;
}

function cbMainGridSortChange( mainGrid, params )
{
	var store = mainGrid.getStore();
	store.setDefaultSort( params.field, params.direction );
	store.reload();
}

function createNumItemsSelector(){
	var store = new Ext.data.SimpleStore({
		fields:['number'],
		data:[[25],[50],[100],[150]]
	});
	var combo = new Ext.form.ComboBox({
		allowBlank:false,
		displayField:'number',
		editable:false,
		maxLength:3,
		maxLengthText:'The maximum value for this field is 999',
		minLength:1,
		minLengthText:'The minimum value for this field is 1',
		mode:'local',
		name:'number',
		selectOnFocus:true,
		store:store,
		triggerAction:'all',
		typeAhead:true,
		value:50,
		width:50
	});
	combo.on({
		'collapse':function() {
			var bb = gMainGrid.getBottomToolbar();
			if( bb.pageSize != combo.value )
			{
				bb.pageSize = combo.value;
				var store = gMainGrid.getStore()
				store.load( { params : { start : 0, limit : bb.pageSize } } );
			}
		}
 	});
	return combo;
}

function createStatusSelector(){
	var store = new Ext.data.SimpleStore({
		fields:['status'],
		data:[['All'],['New'],['Submitted'],['Wait_ssh_context'],['Contextualizing'],['Running'],['Halted'],['Stalled'],['Stopping']]
	});
	var combo = new Ext.form.ComboBox({
		allowBlank:false,
		displayField:'status',
		editable:false,
		mode:'local',
		name:'statusSelector',
		selectOnFocus:true,
		store:store,
		triggerAction:'all',
		typeAhead:true,
		value:'All',
		width:100
	});
	combo.on({
		'collapse':function() {
			var bb = gMainGrid.getBottomToolbar();
			if( bb.statusSelector != combo.value )
			{
				bb.statusSelector = combo.value;
				var store = gMainGrid.getStore()
				store.load();
			}
		}
 	});
	return combo;
}

/*
 * Menus
 */

function cbShowContextMenu( grid, rowId, colId, event )
{
	event.stopEvent();
	gVMMenu.vm_data = grid.getStore().getAt( rowId ).data;
	gVMMenu.vm_instanceID = gVMMenu.vm_data[ 'inst_InstanceID' ];
	gVMMenu.showAt(event.getXY());
}

function cbShowVMHistory( a,b,c )
{
	gVMMenu.hide();
	showInstanceInfoWindow( gVMMenu.vm_data );
}

/*
 *  Callback with Auth or Unauth string, for current RPC access to Web Operation
 */

/*
*/
function checkVmWebOperation()
{
	Ext.Ajax.request({
		url : "checkVmWebOperation",
		cache: false,
		async: false,
		success : ajaxReturn,
		failure : ajaxFailure,
		params : { operation : 'Web' }
	});
}


function ajaxReturn( ajaxResponse, reqArguments )
{
        var retVal = Ext.util.JSON.decode( ajaxResponse.responseText );
        if( ! retVal.OK )
        {
              alert( "Failed to checkVmWebOperation: " + retVal.Message );
              return
        }
	auth_response = retVal.Value 
}

/*
 *  Stopping VMs (stopping mandative management when vmStopPolicy=never, optional if vmStopPolocy=elastic):
 */

function cbStopSelected()
{
	var selIds = getSelectedCheckboxes();
	if( window.confirm( "Are you sure you want to stop selected Virtual Machines?" ) )
		Ext.Ajax.request({
			url : "declareInstancesStopping",
			success : ajaxCBServerStopSelected,
			failure : ajaxFailure,
			params : { idList : Ext.util.JSON.encode( selIds ) }
		});
}

function ajaxCBServerStopSelected( ajaxResponse, reqArguments )
{
	gMainGrid.getStore().reload();
}

function ajaxFailure( ajaxResponse, reqArguments )
{
	alert( "Error in AJAX request : " + ajaxResponse.responseText );
}
