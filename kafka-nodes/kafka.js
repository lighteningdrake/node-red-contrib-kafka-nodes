var kafka = require('kafka-node');

module.exports = function(RED) {    
    /**
     * Kafka Client 
     */
    function KafkaClientNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        
        //define the topic for this client
        var kTopic = config.topic ? config.topic : "nodeRedTest";
        var offset = !isNaN(config.offset) ? parseInt(config.offset,10) : 0;
        
        //create a connection to the Kafka service
        var zClient = new kafka.Client(config.zookeeperServer, 'nodered-kafka-nodes');
        var kConsumer = new kafka.Consumer(zClient, 
            [
                { topic: kTopic, offset: 0 }
            ]);
        node.status({fill:"green",shape:"dot",text:"connected"});
        
        //pass all messages received from the server that match the given topic on.
        kConsumer.on('message', function(msg){
            node.status({fill:"green",shape:"dot",text:"connected"});
            node.log(msg)
            msg.payload = msg.value;
            node.send(msg);
        });
        
        //log errors
        kConsumer.on('error', function(err){
            node.error("error: " + err);
            node.status({fill:"red",shape:"ring",text:"error"});
        });
        
        //close all connections to kafka
        node.on("close", function() {
            kConsumer.close();
            zClient.close();
        });
    }
    RED.nodes.registerType("kafka consumer", KafkaClientNode);
    
    
    function KafkaProducerNode(config){
        RED.nodes.createNode(this, config);
        var node = this;
        
        console.log("inside the kafka producer node...")
        
        //define the topic for this producer
        var kTopic = config.topic ? config.topic : "nodeRedTest";
        console.log("topic(s): " + kTopic);
        
        //producer - create a connection to the Kafka service
        var zClient = new kafka.Client(config.zookeeperServer, 'nodered-kafka-nodes');
        var HighLevelProducer = kafka.HighLevelProducer;
        var kProducer = new HighLevelProducer(zClient);
        node.status({fill:"green",shape:"dot",text:"connected"});
        
        //push a received message from an external service to Kafka
        //Note: these messages can be anything, so we should assume that they could be anything.
        node.on('input', function(msg){
            //create topic if it doesn't exist (ONLY ON SERVER WHERE AUTO CREATING TOPICS IS ENABLED)
            kProducer.createTopics([kTopic], false, function (err, data) {
                if(err){
                    //TODO: HANDLE WHAT TO DO ON A SERVER WITH NO TOPIC CREATION ENABLED
                    console.log(err);
                } else {
                    kProducer.send([{
                        topic: kTopic,
                        messages: msg.payload
                    }]);
                }
            }); 
            node.status({fill:"green",shape:"dot",text:"connected"});
        });
        
        //close all connections to kafka
        node.on("close", function() {
            kProducer.close();
            zClient.close();
        });
    }
    RED.nodes.registerType("kafka producer", KafkaProducerNode);
    
    /**
     * NOTE: kafka-node doesn't support getting topics at this point in time.
     * This function is ignored until that is available.
     */
    RED.httpAdmin.get('/kafka/getTopics', function(req, res, next){
        //check the zookeeperServer for the available topics for this kafka device
        var allStreams = [];
        var zClient = new kafka.Client(config.zookeeperServer, 'nodered-kafka-nodes');
        //TODO: add topics lookup when it becomes available from kafka-node
        
        res.send(allStreams);
    });

}
