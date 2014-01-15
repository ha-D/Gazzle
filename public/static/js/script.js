
var gazzle = {}

gazzle.connect = function(){
	gazzle.ws = new WebSocket("ws://" + location.hostname + ":3300/ws");
	gazzle.socket = gazzle.ws;
	gazzle.frontierSize = 0;
	gazzle.crawlSize = 0;

	var ws = gazzle.ws;

	ws.onmessage = function(evt) {
		console.log("message received: " + evt.data)
		mes = JSON.parse(evt.data);
		if(mes instanceof Array)
			for(var i = 0; i < mes.length; i++)
				gazzle.parseMessage(mes[i]);
		else if(mes instanceof Object)
			gazzle.parseMessage(mes)
	};

	ws.onclose = function(evt) {
		console.log("Connection close");
	};

	ws.onopen = function(evt) { 
		console.log("Connection Opened");
	};

	ws.onerror = function(evt){
		console.log("Connection Error")
		console.log(evt)
	}
}

gazzle.startIncreaseThread = function(){
	function increaseThread(){
		function increase(elem, size){
			var c = parseInt(elem.html())
			var step = 13;
			if(size - c > 100){
				step = parseInt((size - c) / 8);
				if(step < 0)
					step = -step;
			}
			if (c < size){
				c += step;
				if(c > size)
					c = size;
			}else if (c > size){
				c -= step;
				if(c < size)
					c = size;
			}
			elem.html(c);
		}
		increase($("#frontier-size"), frontierSize);
		increase($("#crawl-size"), crawlSize);
		setTimeout(increaseThread, 100);
	}
	setTimeout(increaseThread, 1)
}

gazzle.parseMessage = function(){
	function pageAction(f){
		if(mes.page !== undefined)
			f(mes.page);
		if(mes.pages !== undefined)
			for(var i = 0; i < mes.pages.length; i++)
				f(mes.pages[i]);
	}

	if(mes.action == 'frontier size'){
		frontierSize = mes.value;

	}else if(mes.action == 'crawl size'){
		crawlSize = mes.value;

	}else if(mes.action == 'crawl current'){
		$("#current-crawl").html(mes.page);

	}else if(mes.action == 'crawl page'){	
		pageAction(function(page){
			var div = document.createElement('div');
			$(div).addClass('item');
			$(div).attr('id', 'item-' + mes.page.page_id)
			$(div).attr('data-page-id', mes.page.page_id)
			$(div).html(mes.page.url)
			$('#crawled').prepend($(div));
		});

	}else if(mes.action == 'index page'){
		pageAction(function(page){
			var item = $("#item-"+page.page_id);
			item.addClass("indexed");
		});

	}else if(mes.action == 'index commit'){
		pageAction(function(page){
			var item = $("#item-"+page.page_id);
			item.addClass("commited");
		});
	}
}

$(function(){
	gazzle.connect();
	gazzle.startIncreaseThread();

	$.each($('.url-stats td:not(:has(i)):not(:has(div))'), function(i, d){
		if($(d).html().length > 20 )
			$(d).attr('title', $(d).html());
			$(d).html($(d).html().substring(0,20)+ '...');
	});

	$("#crawl-start-btn").click(function(){
		ws.send(JSON.stringify({
			action: 'start crawl',
			page: $('#crawl-start-text').val()
		}))
	})

	$("#crawl-toggle-btn").click(function(){
		ws.send(JSON.stringify({
			action: 'toggle crawl'
		}))
	})

	$("#index-toggle-btn").click(function(){
		var btn = $("#index-toggle-btn");
		if(btn.attr("data-toggle") == 'start'){
			ws.send(JSON.stringify({
				action: 'start index'
			}))
			btn.attr('data-toggle', 'stop')
			btn.html("Pause Indexing");
		}else if(btn.attr("data-toggle") == 'stop'){
			ws.send(JSON.stringify({
				action: 'stop index'
			}))
			btn.attr('data-toggle', 'start')
			btn.html("Pause Indexing");
		}
	})			
})


	