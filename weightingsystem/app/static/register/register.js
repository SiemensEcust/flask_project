$(document).ready(function(){
	$('.form-group:eq(5)').hide();
	$('#role').on('blur', function(){
		if ( $('#role').val() == '1') {
			$.ajax({
				type:"POST",
	            url: "/user/Eqpquery",
	            DataType:"json",
	            data: {"facID": $('#factoryID').text()},
	            success:function(data){
	            	if (data['state'] == 0) {
	            		alert('请先添加设备！');
	            	}
	            	else{
	            		for (var item in data){
	            			$('#eqp').append("<option value="+ data[item] +">" + data[item] + "</option>")
	            		}
	            	}
	            }
			});
			$('.form-group:eq(5)').show();
		}
	});
})