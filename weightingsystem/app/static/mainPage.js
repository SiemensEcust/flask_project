
function setWidth(){
	var xScroll = $(window).height();
	var mHeight = $('.navbar').height();
	var yScroll = $(window).width();
	// $('#carousel-example-generic').css('height', xScroll);
	$('#PPT').css('height', xScroll - 70);
	$('#PPT').css('width', yScroll);
}

$(document).ready(function(){
	setWidth();
	window.onresize = function(){
		setWidth();
	};
	$('.navbar.navbar-inverse').css('margin-bottom', 0)
	if ($('.alert.alert-warning')) {
		$('.alert.alert-warning').css('margin-bottom', 0)
	}
	
	$('tr').on('click', function(){
		var EqpName = $($(this).children()[0]).text();
		window.location = "./Running/" + EqpName;
	});

})