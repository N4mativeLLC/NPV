/*var table = $('#example').DataTable({
	    columns: [{
	        "title": "customer_id",
	        "data": "customer_id"
	    }, {
	        "title": "customer_name",
	        "data": "customer_name"
	    }]
	});

function getCust_info(){
	var data;
	$.ajax({
	  type: "GET",  
	  url: "/Users/surbhi/Documents/workspace/NPV/src/main/resources/purl_latest.csv",
	  dataType: "text",       
	  success: function(response)  
	  {
		  alert(response);
		data = $.csv.toObjects(response);
		alert("data"+ data);
		//createTable(data);
		table.rows.add(data).draw();
	  }   
	});
}
*/

	


$("#populateTable").click(function() {
	getCust_info();
    
});

var vid = document.getElementById("myVideo"); 

function playVid() { 
	var videoFile = 'https://v.idomoo.com/2498/34254/be39a29c571c4242a20b4a4db3c6eec1989904479adb4510b16c88048785d4db.mp4';
	vid.src = videoFile;
	vid.play(); 
} 

function pauseVid() { 
    vid.pause(); 
}

// /Users/surbhi/Documents/workspace/NPV/src/main/resources/testData20Recs_Limited.csv
function getCust_info() {
	filePath= $("#purlFileName").val();
    $.ajax({
        type: "GET",
        //crossDomain: true,
        url: filePath,//"/Users/surbhi/Documents/workspace/NPV/src/main/resources/purl_latest.csv",
        dataType: "text",
        success: function(data) {processData(data);}
     });
}

function processData(allText) {
    var allTextLines = allText.split(/\r\n|\n/);
    var headers = allTextLines[0].split('|');
    var lines = [];

    for (var i=1; i<allTextLines.length; i++) {
        var data = allTextLines[i].split('|');
        if (data.length == headers.length) {

            var tarr = [];
            for (var j=0; j<headers.length; j++) {
                //tarr.push(headers[j]+":"+data[j]);
            	
                tarr.push(data[j]);
            }
            //alert(tarr);
            //console.log(tarr);
            lines.push(tarr);
        }
    }
    alert(lines);
    fillTable(lines);
    //fillCustInfo(lines);
}
function fillTable(lines){

	$('#example').DataTable( {
	    data: lines,
	    columns: [
	        { title: "Customer_id" },
	        { title: "Custome_Name" },
	        
	        { title: "current_date" },
	        { title: "amount_money_in" },
	        { title: "amount_money_out" },
	        { title: "dates_from_until" },
	        { title: "amount_possible_shortfall" },
	        { title: "possible_cash" },
	        { title: "Upcoming_payments" },
	        { title: "AVAIL_BALANCE" },
	        { title: "video" }
	        
	    ]
	} );
}
function fillCustInfo(lines){
	var cust_id=[];
	var cust_name=[];
	//alert(lines.length);
	for (i=0;i<lines.length;i++){
		cust_id.push(lines[i][0]);
		cust_name.push(lines[i][1]);
	}
	
	for (var i=0;i<cust_id.length;i++){
			   $('<option/>').val(cust_id[i]).html(cust_id[i]).appendTo('#cust_id');
			   $('<option/>').val(cust_name[i]).html(cust_name[i]).appendTo('#cust_name');
			}
		
}