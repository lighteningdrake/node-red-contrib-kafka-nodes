var kafka = require('kafka-node');
var ping = require('ping');

module.exports = function(RED) {    
    /**
     * Kafka Client 
     */
    function KafkaClientNode(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        var zookeeperServer;
        node.kafkaConfig = RED.nodes.getNode(config.address);
        node.status({fill:"yellow",shape:"dot",text:"connecting..."});
        if (node.kafkaConfig) {
            if(node.kafkaConfig.credentials.zookeeperServer){
                zookeeperServer = node.kafkaConfig.credentials.zookeeperServer;
            } else {
                node.error("Credentials missing Zookeeper Server address")
                //potential error checking on server IP address
            }
        }

        //define the topic for this client
        var kTopic = config.topic ? config.topic : "nodeRedTest";
        var offset = !isNaN(config.offset) ? parseInt(config.offset,10) : 0;
        
        //create a connection to the Kafka service
        if(zookeeperServer){
            var hosts = zookeeperServer.split(",");
            console.log(hosts);
            var validConnect = false;
            var amountChecked = 0;
            var kConsumer;
            var zClient;
            
            hosts.forEach(function(host){
                ping.sys.probe(host, function(isAlive){
                    if(isAlive){
                        validConnect = true;
                    }
                    amountChecked++
                    if(amountChecked === hosts.length && validConnect){
                        zClient = new kafka.Client(zookeeperServer, 'nodered-kafka-nodes', [{
                            sessionTimeout: 3000,
                            retries : 0
                        }]);

                        if(zClient){
                            kConsumer = new kafka.Consumer(zClient, 
                                [
                                    { topic: kTopic, offset: 0 }
                                ]);
                            node.status({fill:"green",shape:"dot",text:"connected"});
                        } else {
                            node.error("Client failed to connect");
                            node.status({fill:"red",shape:"ring",text:"error"});
                        }

                        //polling algorithm variables
                        //var retry;
                        //var active = false;
                        //var CONNECTION_RETRY_INTERVAL = 5000;

                        //pass all messages received from the server that match the given topic on.
                        if(kConsumer){
                            kConsumer.on('message', function(msg){
                                node.status({fill:"green",shape:"dot",text:"connected"});
                                node.log(msg.value);
                                msg.payload = msg.value;
                                node.send(msg);
                            });
                        
                            //log errors
                            kConsumer.on('error', function(err){
                                node.error("error: " + err);
                                if(err.toString().indexOf("TopicsNotExist") > -1){
                                    node.status({fill:"yellow",shape:"dot",text:"nonexistant topic"});
                                } else {
                                    node.status({fill:"red",shape:"ring",text:"error"});
                                }
                                // polling system. Doesn't fully work due to nature of kafka-node package.
                                // if(err.toString().indexOf("TopicsNotExist") > -1){
                                //     retry = setInterval(function (){
                                //         kConsumer.removeTopics([kTopic], function(err, removed){
                                //             console.log("removed" + err, removed);
                                //             kConsumer.addTopics([{topic: kTopic, offset:0}], function(err, added){
                                //                 console.log("added" + err, added);
                                //                 if(err === null){
                                //                     clearInterval(retry);
                                //                     node.status({fill:"green",shape:"dot",text:"connected"});
                                //                 }
                                //             });
                                //         });
                                //     }, CONNECTION_RETRY_INTERVAL)
                                // }
                            });
                        }

                    } else if(amountChecked === hosts.length) {
                        node.error("Failed to reach remote server. Have you provided the correct server address?");
                        node.status({fill:"red",shape:"ring",text:"error"});
                    }
                });
            });

        } else {
            node.status({fill:"red",shape:"ring",text:"error"});
        }
            
        //close all connections to kafka
        node.on("close", function() {
            if(kConsumer){
                kConsumer.close();
            }
            if(zClient){
                zClient.close();
            }
        });
    }
    RED.nodes.registerType("kafka consumer", KafkaClientNode);
    
    function KafkaProducerNode(config){
        RED.nodes.createNode(this, config);
        var node = this;
        node.status({fill:"yellow",shape:"dot",text:"connecting..."});
        node.kafkaConfig = RED.nodes.getNode(config.address);
        if (node.kafkaConfig) {
            if(node.kafkaConfig.credentials.zookeeperServer){
                zookeeperServer = node.kafkaConfig.credentials.zookeeperServer;
            } else {
                node.error("Credentials missing Zookeeper Server address")
                //potential error checking on server IP address
            }
        }
        console.log("inside the kafka producer node...")
        
        //define the topic for this producer
        if(config.topic){
            var kTopic = config.topic;
        } else {
            node.error("No topic provided")
        }
        
        //producer - create a connection to the Kafka service
        if(kTopic && zookeeperServer){
            var hosts = zookeeperServer.split(",");
            console.log(hosts);
            var validConnect = false;
            var amountChecked = 0;
            var kProducer;
            var zClient;
            
            hosts.forEach(function(host){
                ping.sys.probe(host, function(isAlive){
                    if(isAlive){
                        validConnect = true;
                    }
                    amountChecked++
                    if(amountChecked === hosts.length && validConnect){
                        zClient = new kafka.Client(zookeeperServer, 'nodered-kafka-nodes', [{
                            sessionTimeout: 3000,
                            retries : 0
                        }]);
                        if(zClient){
                            kProducer = this._kProducer = new kafka.HighLevelProducer(zClient);
                            node.status({fill:"green",shape:"dot",text:"connected"});
                        }
                    } else if(amountChecked === hosts.length) {
                        node.error("Failed to reach remote server. Have you provided the correct server address?");
                        node.status({fill:"red",shape:"ring",text:"error"});
                    }
                });
            });
        }

        //push a received message from an external service to Kafka
        //Note: these messages can be anything, so we should assume that they could be anything.
        node.on('input', function(msg){
            var message;
            if(typeof msg.payload == "object"){
                message = JSON.stringify(msg.payload);
            } else {
                message = String(msg.payload);
            }
            if(zClient){
                kProducer.createTopics([kTopic], false, function(err,data){
                    kProducer.send([{
                        topic: kTopic,
                        //messages: msg
                        messages: message
                    }], function(){
                        node.status({fill:"green",shape:"dot",text:"connected"});
                    });
                });
            } else {
                console.log("Message not sent: Node not correctly configured.");
            }
        });
        
        //close all connections to kafka
        node.on("close", function() {
            if(kConsumer){
                kConsumer.close();
            }
            if(zClient){
                zClient.close();
            }
        });
    }
    RED.nodes.registerType("kafka producer", KafkaProducerNode);
    
    function KafkaCredentialsNode(n) {
        RED.nodes.createNode(this,n);
    }
    RED.nodes.registerType("kafka-credentials",KafkaCredentialsNode, {
        credentials: {
            name: {type:"text"},
            zookeeperServer: {type:"text"}
        }
    });

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