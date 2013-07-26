![Kraken](https://raw.github.com/Asana/kraken/master/images/kraken_128.png)

### Overview

Kraken is a distributed pubsub server that is designed to power collaborative realtime apps like [Asana](http://www.asana.com).

Applications use Kraken to transmit and receive messages through topics. These messages will typically contain just enough information
to identify the set of data that was changed by the client just before the message was published. When other clients
receive these messages, they will figure out which data changed and reload it from the datastore so that they are eventually brought up to date.

Kraken is not a general purpose message bus like [RabbitMQ](http://www.rabbitmq.com/).

### Building Kraken for the first time

We recommend running Kraken with [Erlang R15B03](http://www.erlang.org/download.html) and above, but it will likely work fine with older versions of Erlang as well.

Download the latest Kraken release (or clone the git project), and then use the following command from the root of the Kraken directory to build Kraken for the first time:

    ./rebar get-deps compile

You can now run Kraken in the foreground like this:

    bin/kraken run

This will start Kraken up with the default config. It will listen to new TCP connections on port 12355.

### Running Kraken

Kraken comes with a little bash script that provides a handful of useful commands:

Running Kraken in the foreground, with a live erlang shell

    bin/kraken run

Starting and Stopping Kraken in the background

    bin/kraken start
    bin/kraken stop

Checking if Kraken is currently running in the background

    bin/kraken status

Changing the Kraken log level while it is running

    bin/kraken change_log_level [debug|info|warn|error]

Dumping information about every client queue

    bin/kraken dump_queues

Dumping the list of topics for a particular client's queue

    bin/kraken dump_queue_topics <pid from dump_queues>

Dumping all topics with a count of subscribers

    bin/kraken dump_topics

### Configuring Kraken

Before running Kraken in production, you will want to customize some of the config options. Kraken is built as a standard OTP application, so you can modify config options directly from the command line or by specifying a custom erlang config.

#### Supported options

* **pid_file**: If specified, then the system process id of the erlang node will be written to this file.
* **listen_ip**: The IP address for the Kraken server to listen to new connections on.
* **tcp_server_port**: The port for the Kraken server to listen to new connections on.
* **num_router_shards**: The number of router shards to run. A good starting point is 2x the number of cores on the machine.
* **router_min_fanout_to_warn**: Octopus will log warnings if a message ends up being distributed to this many or more subscribers.

**Specifying options at the command line**

You can specify Kraken options at the command line when starting Kraken as follows:

    bin/kraken run -kraken num_router_shards 8 -kraken router_min_fanout_to_warn 1000

You need to prefix each argument with "-kraken" to let erlang know that you are customizing the kraken application environment. Erlang lets you run multiple applications on a single node.

**Specifying options in a config file**

Kraken options can also be specified in an erlang config file. Here is an example config file:

    [{octopus, [
       {pid_file, "/var/run/kraken.pid"},
       {log_file, "/var/log/kraken.log"},
       {max_tcp_clients, 30000},
       {num_router_shards, 8}]}].

If you stored the config file in /etc/kraken.config, you could tell Erlang to use the config when you start it as follows:

    bin/kraken start -config /etc/kraken

Note that Erlang requires you to exclude the extension when you specify the config file.

### Kraken clients

Kraken currently includes two official clients for Erlang and Node.js. The [Kraken protocol](https://github.com/Asana/Kraken/blob/master/src/kraken_memcached.erl) is based on the Memcached protocol, so it shouldn't take very long to create a client in the language of your choice. Please let us know if you create a new client!

Here is an example of working with Kraken using the [Node.js client](https://github.com/Asana/kraken-node-client):

    js> kraken1 = new Kraken("localhost", 12355);
    js> kranen2 = new Kraken("localhost", 12355);
    js> kraken1.subscribe(["topicA", "topicB"]);
    js> kraken2.publish(["topicA"], "hi there!");
    js> console.log(kraken1.receiveMessages());
    js> kraken2.unsubscribe(["topicA"]);

As you see above, the Memcached based protocol requires clients to poll for new messages. Most good message bus proctocols (like AMQP) have some kind of polling in the form of a heart beat so that clients can detect dead connections sooner than later. In Kraken, the receive command is the way to receive new messages and the heartbeat at the same time. A decent machine should be able to handle thousands of clients polling once every couple of seconds without a problem. It probably wouldn't take very long to add a new protocol to Kraken that pushes messages to clients if you need it! Kraken was designed with the goal of supporting multiple protocols in the future.

### How do I use Kraken to build realtime apps?

Kraken was designed to forward data invalidation messages between application servers. It's up to the application designer to figure out how to scope these messages to topics, and what they should contain. For example, a simple TODO list app may have topics corresponding to each of the lists that a user can see. This app would publish invalidation messages corresponding to the ids of tasks that have changed through the topics corresponding to the lists that the task is and was a member of. When other application servers receive these messages, they would reload the state of the tasks referenced in the invalidations messages to ensure they are still up to date.

### How does Kraken scale?

As far as we know, very well. Kraken has been powering the Asana service since mid 2010, and has yet to crash or fail in any way. At Asana, we have 10s of thousands of clients connected to each Kraken node.

There are two ways of scaling Kraken beyond a single machine:

1. You can shard the topic space so that each machine is responsible for a portion of the topics. This will typically decrease the total number of messages that a given node needs to process and reduce the amount of memory required to keep track of all the routing information.

2. You can run Kraken nodes that proxy to other Kraken nodes. The proxy nodes will aggregate connections and routing information from their clients and forward on the minimal amount of information necessary to ensure they stay up to date. The proxy nodes then become a single client to the Kraken nodes that they connect to, substantially decreasing the total number of clients and messages that any single Kraken node needs to handle!

### Authors and Contributors

Kraken was developed at [Asana](http://www.asana.com/jobs).
The original version was written by Kris Rasmussen ([@krisr](https://github.com/krisr)) in 2010.
It was rewritten to be retroactive by Samvit Ramadurgam ([@samvit](https://github.com/samvit)) in 2013.
The Kraken mascot was designed by Stephanie Hornung.

### Support or Contact
Having trouble with Kraken? Check out the documentation at https://github.com/Asana/Kraken/wiki or file an issue at https://github.com/Asana/Kraken/issues and we’ll help you sort it out.

### Current Development

* **Retroactive Subscription**: Sometimes clients want to subscribe to topics retroactively and receive messages that have already flown through the system.
This is useful for situations where clients want to avoid repeated synchonous roundtrips to kraken as the set of topics they are interested in expands,
but don't want to miss out on messages that get sent between the times when a subscription is needed and when the batch-subscription is actually established.
