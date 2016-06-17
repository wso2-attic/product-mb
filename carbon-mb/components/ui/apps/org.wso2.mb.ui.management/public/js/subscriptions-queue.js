$(document).ready(function() {
    $('#queue-subscription-protocols').change(function(){
        var selectedProtocol = $("#queue-subscription-protocols option:selected").text();
        var redirectURL = window.location.href.split('?')[0] + "?protocol=" + selectedProtocol;
        window.location.href = redirectURL;
    })
});