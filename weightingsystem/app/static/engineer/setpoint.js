// function sendData(){
// 	var Num = $('#Num').val();
//     var ExcV = $('#ExcV').val();
//     var Sensitivity = $('#Sensitivity').val();
//     var Resistance = $('#Resistance').val();
//     var Temp = $('#Temp').val();
//     var Wet = $('#Wet').val();
//     var Name = $('#Name').val();
//     var noLoad = $('#noLoad').val();
//     var emptyLoad = $('#emptyLoad').val();
//     var eqpName = $('#eqpName').val();
//     var supplier = $('#productInfo').val();
//     var str_param_len1 = noLoad.split(',').length;
//     var str_param_len2 = emptyLoad.split(',').length;
//     var str_param_len3 = Name.split(',').length;
//     var data = {"Num": Num, "ExcV": ExcV, "Sensitivity": Sensitivity, "Resistance": Resistance, "noLoad": noLoad, "Temp": Temp, "Wet": Wet, "Name": Name, "emptyLoad": emptyLoad, "eqpname": eqpName, "supplier": supplier};
//     if (str_param_len1 != Number(Num)) {
//     	alert('请确认无负载读数数量是否与传感器个数相符');
//     	$('#submit').removeAttr("disabled");
//     }
//     if (str_param_len2 != Number(Num)) {
//     	alert('请确认空载读数数量是否与传感器个数相符');
//     	$('#submit').removeAttr("disabled");
//     }
//     if (str_param_len2 != Number(Num)) {
//     	alert('请确认传感器位号数量是否与传感器个数相符');
//     	$('#submit').removeAttr("disabled");
//     }
//     if (str_param_len1 == Number(Num) && str_param_len2 == Number(Num) && str_param_len3 == Number(Num)) {
//     	$.ajax({
//             type:"POST",
//             url: "/scale/engineer/insertSetpoint",
//             DataType:"json",
//             data:data,
//             success:function(data){
//             	if (data.success) {
//             		alert('上传参数成功！');
//             		$('#submit').removeAttr("disabled");
//             	}
//             	else{
//             		alert('上传参数失败！');
//             		$('#submit').removeAttr("disabled");
//             	}
//             }
//         });
//     }
// }

// var label = 1;
$(document).ready(function(){
	$('div.form-group.required').each(function(){
		$(this).addClass('col-md-6 col-sm-6');
		$(this).css({
			'margin-bottom': 10,
		});
	})
	$('#submit').addClass('col-md-offset-3 col-sm-offset-3 col-md-4 col-sm-4');

	// $('#submit').on('click', function(e){
	// 	$('#submit').attr("disabled", "disabled");
	// 	$('input').each(function(){
	// 		if (!($(this).val())) {
	// 			label = 0;
	// 		}
	// 	})
	// 	// e.prevenrDefault();
	// 	if (label) {
	// 		sendData();
	// 	}
	// 	else{
	// 		alert('请按要求填写参数，若无此参数请填写无！');
	// 		$('#submit').removeAttr("disabled");
	// 		label = 1;
	// 	}
		
	// })

})