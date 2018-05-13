var subscription = null;
var trendsSubscription = null;
var newQuery = 0;

function registerTemplates() {
	template = $("#template").html();
	Mustache.parse(template);

    trendsTemplate = $("#trendsTemplate").html();
    Mustache.parse(trendsTemplate);
}

function setConnected(connected) {
	var search = $('#submitsearch');
	search.prop('disabled', !connected);
}

function suscribeToTrends(stompClient) {
    trendsSubscription = stompClient.subscribe("/queue/trends", function (data) {
        var trends = JSON.parse(data.body);

        // Transforma el formato de la lista para mapearlo con Mustache
        var trendsKeyValues = [];
        trends.forEach(function (trend) {
            Object.entries(trend).forEach(function (keyValuePair) {
                trendsKeyValues.push({'key': keyValuePair[0], 'val': keyValuePair[1]});
			})
        });

        $("#trendsBlock").html(Mustache.render(trendsTemplate, {"trends": trendsKeyValues}));
    });
}

function registerSendQueryAndConnect() {
    var socket = new SockJS("/twitter");
    var stompClient = Stomp.over(socket);
    stompClient.connect({}, function(frame) {
        setConnected(true);
        suscribeToTrends(stompClient);
        console.log('Connected: ' + frame);
    });
	$("#search").submit(
			function(event) {
				event.preventDefault();
				if (subscription) {
					subscription.unsubscribe();
				}
				var query = $("#q").val();
				stompClient.send("/app/search", {}, query);
				newQuery = 1;
				subscription = stompClient.subscribe("/queue/search/" + query, function(data) {
					var resultsBlock = $("#resultsBlock");
					if (newQuery) {
                        resultsBlock.empty();
						newQuery = 0;
					}
					var tweet = JSON.parse(data.body);
                    resultsBlock.prepend(Mustache.render(template, tweet));
				});
			});
}

$(document).ready(function() {
	registerTemplates();
	registerSendQueryAndConnect();
});
