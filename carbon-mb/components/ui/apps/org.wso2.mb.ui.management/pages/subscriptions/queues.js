var onRequest = function (context) {
    //Getting supported protocols
    var protocols = callOSGiService("org.wso2.andes.kernel.Andes", "getSupportedProtocols", []);
    var protocolStrings = [];
    for each (var item in protocols) {
        protocolStrings.push(item.toString());
    }
    var protocolsJson = JSON.stringify(protocolStrings);

    //Getting subscriptions
    var queryParamProtocol = context.request.queryParams["protocol"];
    if (queryParamProtocol != null && queryParamProtocol != "") {
        //getSubscriptions(ProtocolType protocol, DestinationType destinationType, String subscriptionName, String destinationName, boolean active, int offset, int limit)

        var andesResourceManager = callOSGiService("org.wso2.andes.kernel.Andes", "getAndesResourceManager", []);
        var protocolClass = Java.type("org.wso2.andes.kernel.ProtocolType");
        var protocolInstance = new protocolClass(queryParamProtocol);
        var destinationTypeEnum = Java.type("org.wso2.andes.kernel.DestinationType");
        var subscriptions = andesResourceManager.getSubscriptions(protocolInstance, destinationTypeEnum.QUEUE, "*", "*", "*", 0, 1000);
        print("Subscriptions : " + subscriptions);
    }

    return {"protocols" : protocolsJson};
};
