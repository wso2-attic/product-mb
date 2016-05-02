$(document).ready(function(){
    getQueuesToTable();
});

function getQueuesToTable() {
    $.ajax({
        type: 'GET',
        url: getBaseUrl() + "/mb/v1.0.0/amqp/destination-type/queue",
        dataType: 'json',
        success:function(data) {
            $.each(data, function(i, queue) {
                $("#queue-list-table").find('tbody')
                                .append($('<tr>')
                                    .append($('<td>')
                                        .append(queue.destinationName)
                                    )
                                    .append($('<td>')
                                        .append(queue.messageCount)
                                    )
                                    .append($('<td>')
                                        .append(queue.subscriptionCount)
                                    )
                                    .append($('<td>')
                                        .append(queue.owner)
                                    )
                                    .append($('<td>')
                                        .append(getPurgeAndDeleteButtons(queue.destinationName))
                                    )
                                );
            });

        },
        error: function( req, status, err ) {
            console.log( 'something went wrong', status, err );
        }
    });
}

function getPurgeAndDeleteButtons(queueName) {
    return `<a href="#" onclick="showPurgeConfirmation(\'` + queueName + `\');" data-click-event="remove-form"
    class="btn padding-reduce-on-grid-view">
    <span class="fw-stack">
        <i class="fw fw-ring fw-stack-2x"></i>
        <i class="fw fw-delete fw-stack-1x"></i>
    </span>
    <span class="hidden-xs">Purge</span></a><a href="#"
    onclick="showDeleteConfirmation(\'` + queueName + `\');" data-click-event="remove-form"
    class="btn padding-reduce-on-grid-view">
    <span class="fw-stack">
        <i class="fw fw-ring fw-stack-2x"></i>
        <i class="fw fw-delete fw-stack-1x"></i>
    </span>
    <span class="hidden-xs">Delete</span></a>`;
}

function showPurgeConfirmation(queueName) {
    // Binding purge for each row on table
    $("#success-alert").hide();
    $("#error-alert").hide();
    $("#purge-queue-name").val(queueName);
    $("#purge-button").data("purge-button-queue-name", queueName);
    $("#modalPurge").modal('show');
}

function showDeleteConfirmation(queueName) {
    // Binding delete for each row on table
    $("#success-alert").hide();
    $("#error-alert").hide();
    $("#delete-queue-name").val(queueName);
    $("#delete-button").data("delete-button-queue-name", queueName);
    $("#modalDelete").modal('show');
}

function clearQueueTable() {
    $("#queue-list-table tbody").remove();
}

function purgeQueue(input) {
    var queueName = $(input).data("purge-button-queue-name");
    $.ajax({
            type: 'DELETE',
            url: getBaseUrl() + "/mb/v1.0.0/amqp/destination-type/queue/name/" + queueName + "/messages",
            dataType: 'json',
            success:function() {
                $("#success-alert").show();
                $("#success-message").text("Queue '" + queueName + "' was successfully purged");
                clearQueueTable();
                getQueuesToTable();
                $("#modalPurge").modal('hide');
            },
            error: function( req, status, err ) {
                $("#error-message").text("Error purging queue '" + queueName + "'.");
                $("#error-alert").show();
        }
    });
}

function deleteQueue(input) {
    var queueName = $(input).data("delete-button-queue-name");
    $.ajax({
            type: 'DELETE',
            url: getBaseUrl() + "/mb/v1.0.0/amqp/destination-type/queue/name/" + queueName,
            dataType: 'json',
            success:function() {
                $("#success-alert").show();
                $("#success-message").text("Queue '" + queueName + "' was successfully deleted");
                clearQueueTable();
                getQueuesToTable();
                $("#modalDelete").modal('hide');
            },
            error: function( req, status, err ) {
                $("#error-message").text("Error deleting queue '" + queueName + "'.");
                $("#error-alert").show();
        }
    });
}