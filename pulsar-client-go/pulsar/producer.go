//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

package pulsar

type MessageRoutingMode int

const (
	// The producer will chose one single partition and publish all the messages into that partition
	UseSinglePartition MessageRoutingMode = 0

	// Publish messages across all partitions in round-robin.
	RoundRobinDistribution MessageRoutingMode = 1

	// Use custom message router implementation that will be called to determine the partition for a particular message.
	CustomPartition MessageRoutingMode = 2
)

type HashingScheme int

const (
	Murmur3_32Hash HashingScheme = 0 // Use Murmur3 hashing function
	BoostHash      HashingScheme = 1 // C++ based boost::hash
	JavaStringHash HashingScheme = 2 // Java String.hashCode() equivalent
)

type CompressionType int

const (
	NoCompression CompressionType = 0
	LZ4           CompressionType = 1
	ZLib          CompressionType = 2
)

type TopicMetadata interface {
	// Get the number of partitions for the specific topic
	NumPartitions() int
}

type ProducerBuilder interface {
	// Create the producer instance
	// This method will block until the producer is created successfully
	Create() (Producer, error)

	// Create the producer instance in asynchronous mode. The callback
	// will be triggered once the operation is completed
	CreateAsync(callback func(producer Producer, err error))

	// Specify the topic this producer will be publishing on.
	// This argument is required when constructing the producer.
	Topic(topic string) ProducerBuilder

	// Specify a name for the producer
	// If not assigned, the system will generate a globally unique name which can be access with
	// Producer.ProducerName().
	// When specifying a name, it is up to the user to ensure that, for a given topic, the producer name is unique
	// across all Pulsar's clusters. Brokers will enforce that only a single producer a given name can be publishing on
	// a topic.
	ProducerName(producerName string) ProducerBuilder

	// Set the send timeout (default: 30 seconds)
	// If a message is not acknowledged by the server before the sendTimeout expires, an error will be reported.
	// Setting the timeout to zero, will set the timeout to infinity, which can be useful when using Pulsar's message
	// deduplication feature.
	SendTimeout(sendTimeoutMillis int) ProducerBuilder

	// Set the max size of the queue holding the messages pending to receive an acknowledgment from the broker.
	// When the queue is full, by default, all calls to Producer.send() and Producer.sendAsync() will fail
	// unless `BlockIfQueueFull` is set to true. Use BlockIfQueueFull(boolean) to change the blocking behavior.
	MaxPendingMessages(maxPendingMessages int) ProducerBuilder

	// Set the number of max pending messages across all the partitions
	// This setting will be used to lower the max pending messages for each partition
	// `MaxPendingMessages(int)`, if the total exceeds the configured value.
	MaxPendingMessagesAcrossPartitions(maxPendingMessagesAcrossPartitions int) ProducerBuilder

	// Set whether the `Producer.Send()` and `Producer.sendAsync()` operations should block when the outgoing
	// message queue is full. Default is `false`. If set to `false`, send operations will immediately fail with
	// `ProducerQueueIsFullError` when there is no space left in pending queue.
	BlockIfQueueFull(blockIfQueueFull bool) ProducerBuilder

	// Set the message routing mode for the partitioned producer.
	// Default routing mode is round-robin routing.
	//
	// This logic is applied when the application is not setting a key MessageBuilder#setKey(String) on a
	// particular message.
	MessageRoutingMode(messageRoutingMode MessageRoutingMode) ProducerBuilder

	// Change the `HashingScheme` used to chose the partition on where to publish a particular message.
	// Standard hashing functions available are:
	//
	//  - `JavaStringHash` : Java String.hashCode() equivalent
	//  - `Murmur3_32Hash` : Use Murmur3 hashing function.
	// 		https://en.wikipedia.org/wiki/MurmurHash">https://en.wikipedia.org/wiki/MurmurHash
	//  - `BoostHash`      : C++ based boost::hash
	//
	// Default is `JavaStringHash`.
	HashingScheme(hashingScheme HashingScheme) ProducerBuilder

	// Set the compression type for the producer.
	// By default, message payloads are not compressed. Supported compression types are:
	//  - LZ4
	//  - ZLIB
	CompressionType(compressionType CompressionType) ProducerBuilder

	// Set a custom message routing policy by passing an implementation of MessageRouter
	// The router is a function that given a particular message and the topic metadata, returns the
	// partition index where the message should be routed to
	MessageRouter(messageRouter func(msg Message, metadata TopicMetadata) int) ProducerBuilder

	// Control whether automatic batching of messages is enabled for the producer. Default: false [No batching]
	//
	// When batching is enabled, multiple calls to Producer.sendAsync can result in a single batch to be sent to the
	// broker, leading to better throughput, especially when publishing small messages. If compression is enabled,
	// messages will be compressed at the batch level, leading to a much better compression ratio for similar headers or
	// contents.
	//
	// When enabled default batch delay is set to 1 ms and default batch size is 1000 messages
	EnableBatching(enableBatching bool) ProducerBuilder

	// Set the time period within which the messages sent will be batched (default: 10ms) if batch messages are
	// enabled. If set to a non zero value, messages will be queued until this time interval or until
	BatchingMaxPublishDelay(batchDelayMillis uint64) ProducerBuilder

	// Set the maximum number of messages permitted in a batch. (default: 1000) If set to a value greater than 1,
	// messages will be queued until this threshold is reached or batch interval has elapsed
	BatchingMaxMessages(batchMessagesMaxMessagesPerBatch uint) ProducerBuilder

	// Set the baseline for the sequence ids for messages published by the producer.
	// First message will be using `(initialSequenceId + 1)` as its sequence id and subsequent messages will be assigned
	// incremental sequence ids, if not otherwise specified.
	InitialSequenceId(initialSequenceId int64) ProducerBuilder
}

// Callback type for asynchronous operations
type Callback func(err error)

// The producer is used to publish messages on a topic
type Producer interface {
	// return the topic to which producer is publishing to
	Topic() string

	// return the producer name which could have been assigned by the system or specified by the client
	ProducerName() string

	// Send a simple message
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Use Send() to specify more properties than just the value on the message to be sent.
	SendBytes(message []byte) error

	// Send a message in asynchronous mode
	SendBytesAsync(payload []byte, callback Callback)

	// Send a message
	// This call will be blocking until is successfully acknowledged by the Pulsar broker.
	// Example:
	// producer.Send(NewMessage().Payload(myPayload).Property("a", "1").Build())
	Send(msg Message) error

	// Send a message in asynchronous mode
	SendAsync(msg Message, callback Callback)

	// Close the producer and releases resources allocated
	// No more writes will be accepted from this producer. Waits until all pending write request are persisted. In case
	// of errors, pending writes will not be retried.
	Close() error

	// Close the producer in asynchronous mode
	CloseAsync(callback Callback)
}
