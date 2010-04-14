var gMainGrid = false;
var gVMMenu = false;

function initVMBrowser(){
  Ext.onReady(function(){
    renderPage();
  });
}

function renderPage()
{
	var reader = new Ext.data.JsonReader({
		root : 'instances',
		totalProperty : 'numRecords',
		id : 'inst_VMInstanceID',
		fields : [ "inst_Name", "inst_VMInstanceID", "inst_ErrorMessage", "inst_Status", "inst_UniqueID", 
		           "img_VMImageID", "img_Name", "inst_VMImageID", "inst_PublicIP", "img_Flavor", 'inst_LastUpdate',
		           'hist_Load']
    });

	var store = new Ext.data.Store({
		reader: reader,
		url : "getInstancesList",
		autoLoad : true,
		sortInfo: { field: 'inst_LastUpdate', direction: 'DESC' },
		listeners : { 
			beforeload : cbStoreBeforeLoad		
	    },
	});

	gMainGrid = new Ext.grid.GridPanel( {
		store : store,
		/*view: new Ext.grid.GroupingView({
			groupTextTpl: '{text} ({[values.rs.length]} {[values.rs.length > 1 ? "Items" : "Item"]})',
			emptyText: 'No data',
			startCollapsed : false,
		}),*/
		columns: [
		    { id : 'check', header : '', width : 30, dataIndex: 'inst_VMInstanceID', renderer : renderSelect },
            { header: "Image", width: 100, sortable: true, dataIndex: 'img_Name'},
            { header: "Status", width: 60, sortable: true, dataIndex: 'inst_Status'},
            { header: "ID", width: 80, sortable: true, dataIndex: 'inst_UniqueID'},
            { header: "IP", width: 100, sortable: true, dataIndex: 'inst_PublicIP'},
            { header: "Load", width: 50, sortable: true, dataIndex: 'hist_Load'},
            { header: "Flavor", width: 100, sortable: true, dataIndex: 'img_Flavor'},
            { header: "Last Update (UTC)", width: 150, sortable: true, dataIndex: 'inst_LastUpdate' },
            { header: "Error", width: 350, sortable: true, dataIndex: 'inst_ErrorMessage'},
        ],
        region : 'center',
        tbar : [
   				{ handler:function(){ toggleAll(true) }, text:'Select all', width:150, tooltip:'Click to select all rows' },
   				{ handler:function(){ toggleAll(false) }, text:'Select none', width:150, tooltip:'Click to select all rows' },
   				'->',
      			//{ handler:function(){ cbDeleteSelected() }, text:'Delete', width:150, tooltip:'Click to delete all selected proxies' },
      	],
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
	
	gVMMenu = new Ext.menu.Menu({
	   	id : 'OptionContextualMenu',
	   	items : [ { text : 'Show history', listeners : { click : cbShowVMHistory }
  				 },
   			  ]
	   })
	gMainGrid.on( 'cellcontextmenu', cbShowContextMenu );
	
	renderInMainViewport( [gMainGrid] );
}

function renderSelect( value, metadata, record, rowIndex, colIndex, store )
{
	return '<input id="' + record.id + '" type="checkbox"/>';
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
		data:[['All'],['New'],['Submitted'],['Running'],['Halted'],['Stalled'],['Error']]
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
	gVMMenu.vm_instanceID = gVMMenu.vm_data[ 'inst_VMInstanceID' ];
	gVMMenu.showAt(event.getXY());
}

function cbShowVMHistory( a,b,c )
{
	gVMMenu.hide();
	showInstanceHistoryWindow( gVMMenu.vm_data[ 'inst_UniqueID' ], gVMMenu.vm_instanceID );
}


/*
 * History window
 */

function showInstanceHistoryWindow( uniqueID, instanceID )
{
	var items = [];
	var historyLogPanel = new Ext.grid.GridPanel({
		store : new Ext.data.JsonStore({
			url : 'getInstanceHistory',
			root : 'history',
			fields : [ 'Status', 'Load', 'Jobs', 'TransferredFiles', 'TransferredBytes', 'Update' ],
			autoLoad : true,
			baseParams : { instanceID : instanceID }
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
	    title : 'VM '+uniqueID,
	    items : [ tabPanel ]
	  })
	extendedInfoWindow.show();
}

/*
 * OLD DELETE
 */

function cbDeleteSelected()
{
	var selIds = getSelectedCheckboxes()
	if( window.confirm( "Are you sure you want to delete selected proxies?" ) )
		Ext.Ajax.request({
			url : "deleteProxies",
			success : ajaxCBServerDeleteSelected,
			failure : ajaxFailure,
			params : { idList : Ext.util.JSON.encode( selIds ) },
		});
}

function ajaxCBServerDeleteSelected( ajaxResponse, reqArguments )
{
	var retVal = Ext.util.JSON.decode( ajaxResponse.responseText );
	if( ! retVal.OK )
	{
		alert( "Failed to delete proxies: " + retVal.Message );
	}
	else
		alert( "Deleted " + retVal.Value + " proxies" );
	gMainGrid.getStore().reload();
}

function ajaxFailure( ajaxResponse, reqArguments )
{
	alert( "Error in AJAX request : " + ajaxResponse.responseText );
}