$(document).ready(function(){
	$.each($('.url-stats td:not(:has(i)):not(:has(div))'), function(i, d){
		if($(d).html().length > 20 )
			$(d).attr('title', $(d).html());
			$(d).html($(d).html().substring(0,20)+ '...');
	});
});

	