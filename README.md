# node-red-contrib-kafka-nodes
Nodes integrating Apache Kafka inside NodeRed.  There are separate nodes for the producer and consumer.

Welcome to kafka-nodes!  This is an implementation of an Apache Kafka consumer and producer clients inside of NodeRed.  These enable you to gather and use events that would pass through a Kafka server.

Before using, you must install the kafka-node package in your NodeRed project:
  npm install kafka-node
It's easiest to modify the package.json by adding: "kafka-node":"^0.2.14" to the dependencies.

It's easy to use: drag the appropriate node to the palate, fill out the Zookeeper server information, wire it to your other nodes, and deploy.  These are not Kafka or Zookeeper managment nodes, and there's no included server.  Check out the Apache Kafka documentation for setting up the server.

The nodes work with NodeRed 10.1 and above.  I haven't tested it in embedded NodeRed; if you try it and ecounter any issues, please let me know.  The Kafka integration is based on the kafka-node package.

Future Enhancements
Here's a short list of things that are planned for the future.  If you would like to see something new included, please submit a pull request.
  + Producer - Send messages to multiple topics
  + Consumer - Subscribe to multiple topics
  + Security - as provided from Kafka itself
