var gMainGrid = false;
var gVMMenu = false;

function initVMDashboard(){
  Ext.onReady(function(){
    renderPage();
  });
}

function renderPage()
{
	
	renderInMainViewport( [] );
}
