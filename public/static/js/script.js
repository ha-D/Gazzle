
var gazzle = {}

gazzle.alert = function(mes, type, time){
	if(time === undefined){
		time = 3000;
	}
	var alert = $("<div>");
	alert.addClass('alert');
	if (type !== undefined)
		alert.addClass(type);
	alert.html(mes);
	$('.alert.stack').prepend(alert);
	setTimeout(function(){
		alert.remove();
	}, time)
}

gazzle.error = function(mes){
	gazzle.alert(mes, 'error');
}

gazzle.success = function(mes){
	gazzle.alert(mes, 'success');
}

gazzle.connect = function(reconnectTime){
	if(reconnectTime === undefined)
		reconnectTime = 5000;
	gazzle.ws = new WebSocket("ws://" + location.hostname + ":3300/ws");
	gazzle.socket = gazzle.ws;
	gazzle.frontierSize = 0;
	gazzle.crawlSize = 0;

	var ws = gazzle.ws;

	ws.onmessage = function(evt) {
		// console.log("message received: " + evt.data)
		mes = JSON.parse(evt.data);
		if(mes instanceof Array)
			for(var i = 0; i < mes.length; i++)
				gazzle.parseMessage(mes[i]);
		else if(mes instanceof Object)
			gazzle.parseMessage(mes)
	};

	ws.onclose = function(evt) {
		console.log("Connection Closed");
		gazzle.error("Connection Closed");
		$("#status").html("not connected");
		$('#status').addClass("error");
		setTimeout(function(){
			gazzle.alert("Reconnecting in " + reconnectTime/1000 +" seconds...", '', reconnectTime);
			setTimeout(function(){
				gazzle.connect(reconnectTime * 2);
			}, reconnectTime + 2500);
		}, 1000)
	};

	ws.onopen = function(evt) { 
		console.log("Connection Opened");
		gazzle.success("Connected To GaZzle");
		$("#status").html("connected");
		$('#status').removeClass("error");
	};

	ws.onerror = function(evt){
		// gazzle.error("Error connecting to GaZzle");
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
		increase($("#frontier-size"), gazzle.frontierSize);
		increase($("#crawl-size"), gazzle.crawlSize);
		setTimeout(increaseThread, 100);
	}
	setTimeout(increaseThread, 1)
}

gazzle.parseMessage = function(mes){
	function pageAction(mes, f){
		if(mes.page !== undefined)
			f(mes.page);
		if(mes.pages !== undefined)
			for(var i = 0; i < mes.pages.length; i++)
				f(mes.pages[i]);
	}

	function addToList(page){
		var tr = document.createElement('tr');
		var title = document.createElement('td');
		var index = document.createElement('td');
		var a = document.createElement('a');
		$(a).attr('href', page.url);
		$(a).html(page.title);
		$(title).html(a);
		$(title).addClass('page title ' + page.page_id);
		$(index).addClass('page index status ' + page.page_id);
		var icon = document.createElement('i');
		if(page.indexed !== undefined && page.indexed == true){
			$(index).attr('data-status', 'indexed');
			$(icon).addClass('icon checkmark');	
		}else{
			$(index).attr('data-status', 'not indexed');
			$(icon).addClass('icon remove');
		}
		$(index).html(icon);
		$(index).attr('data-page', page.page_id);
		$(tr).addClass('page ' + page.page_id);
		$(tr).append(title);
		$(tr).append(index);
		$('.table.page tbody').prepend($(tr));
	}

	if(mes.action == 'frontier size'){
		gazzle.frontierSize = mes.value;

	}else if(mes.action == 'crawl size'){
		gazzle.crawlSize = mes.value;

	}else if(mes.action == 'crawl current'){
		$("#current-crawl").html(mes.page);

	}else if(mes.action == 'crawl page'){	
		pageAction(mes, addToList);

	}else if(mes.action == 'index page'){
		pageAction(mes, function(page){
			var item = $(".page.index.status." +  page.page_id);
			item.attr("data-status", "index pending");
			var icon = document.createElement('i');
			$(icon).addClass('icon loading');
			item.html(icon);
		});

	} else if(mes.action == 'index commit'){
		pageAction(mes, function(page){
			var item = $(".page.index.status." +  page.page_id);
			item.attr("data-status", "indexed");
			var icon = document.createElement('i');
			$(icon).addClass('icon checkmark');
			item.html(icon);
		});
	} else if(mes.action == 'index clear'){
		$('.page.index.status').html("")
		$('.page.index.status').attr("data-status", "not indexed");
	} else if(mes.action == 'search results'){
		var resultList = $("#results");
		resultList.html("");
		for(var i = 0; i < mes.results.length; i++){
			var li = $("<li>");
			var a = $("<a>");
			a.attr('href', mes.results[i].url);
			a.html(mes.results[i].title);
			li.html(a);
			resultList.append(li);
		}
	} else if(mes.action == 'init'){
		if(mes.pages !== undefined){
			$('.table.page tbody').html('')
			pageAction(mes, addToList);
		}
		if(mes.frontier_size !== undefined){
			gazzle.frontierSize = mes.frontier_size;
			$("#frontier-size").html(gazzle.frontierSize);
		}
		if(mes.crawl_size !== undefined){
			gazzle.crawlSize = mes.crawl_size;
			$("#crawl-size").html(gazzle.crawlSize);
		}
		if(mes.indexing !== undefined && mes.indexing == true){
			// TODO
		}
	}
}
$(function(){
	gazzle.connect();
	gazzle.startIncreaseThread();

	var ws = gazzle.socket;

	$.each($('.url-stats td:not(:has(i)):not(:has(div))'), function(i, d){
		if($(d).html().length > 20 )
			$(d).attr('title', $(d).html());
			$(d).html($(d).html().substring(0,20)+ '...');
	});

	$(".table.page").on('mouseenter', '.page.index.status', function(e){
		var state = $(this).attr("data-status");
		if(state == 'not indexed'){
			var icon = document.createElement('i');
			$(icon).addClass('icon add');
			$(this).html(icon);
		}
	})

	$(".table.page").on('mouseleave', '.page.index.status', function(e){
		var state = $(this).attr("data-status");
		if(state == 'not indexed'){
			var icon = document.createElement('i');
			$(icon).addClass('icon remove');
			$(this).html(icon);
		}
	})

	$(".table.page").on('click', '.page.index.status .icon.add', function(e){
		var index = $(this).parent();
		var page = index.attr('data-page');
		ws.send(JSON.stringify({
			action: 'index page',
			page: parseInt(page)
		}))
	})

	$("#searchbox").keydown(function(event){
		if (event.keyCode == 13) {
			console.log("Searching " + $(this).val());
			ws.send(JSON.stringify({
				action: 'search',
				query: $(this).val()
			}))
		}
	})
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

	$("#index-clear-btn").click(function(){
		ws.send(JSON.stringify({
			action: 'clear index'
		}))
	})

	$("#frontier-clear-btn").click(function(){
		ws.send(JSON.stringify({
			action: 'clear frontier'
		}))
	})

	$("#all-clear-btn").click(function(){
		ws.send(JSON.stringify({
			action: 'clear all'
		}))
	})
})


	