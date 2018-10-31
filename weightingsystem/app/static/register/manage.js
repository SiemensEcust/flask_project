$(document).ready(function(){
	$('tr').on('click', function(){
		var username = $($(this).children()[0]).text();
		window.location = "./editUser/" + username;
	});
})