$(document).ready(function(){
    $("#new-queue-form").submit(function(e){
        e.preventDefault(); // this will prevent from submitting the form.
        $.ajax({
            type: 'POST',
            url: getBaseUrl() + "/mb/v1.0.0/amqp/destination-type/queue/name/" + $("#queue-name").val(),
            success:function(data){
                $("#new-queue-success-modal").modal('show');
                $("#error-alert").hide();
                $("#new-queue-success-message").text("Queue '" + $("#queue-name").val() + "' was successfully created.");
            },
            error: function( req, status, err ) {
                $("#new-queue-success-modal").modal('hide');
                $("#error-alert").show();
            }
        });
    });
});