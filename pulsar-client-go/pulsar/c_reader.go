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

/*
#cgo CFLAGS: -I../../pulsar-client-cpp/include
#cgo LDFLAGS: -lpulsar -L../../pulsar-client-cpp/lib
#include "c_go_pulsar.h"
*/
import "C"

import (
	"errors"
	"github.com/mattn/go-pointer"
	"runtime"
	"unsafe"
	"context"
)

type reader struct {
	ptr            *C.pulsar_reader_t
	defaultChannel chan ReaderMessage
}

func readerFinalizer(c *reader) {
	if c.ptr != nil {
		C.pulsar_reader_free(c.ptr)
	}
}

//export pulsarCreateReaderCallbackProxy
func pulsarCreateReaderCallbackProxy(res C.pulsar_result, ptr *C.pulsar_reader_t, ctx unsafe.Pointer) {
	cc := pointer.Restore(ctx).(*readerAndCallback)

	C.pulsar_reader_configuration_free(cc.conf)

	if res != C.pulsar_result_Ok {
		cc.callback(nil, newError(res, "Failed to create Reader"))
	} else {
		cc.reader.ptr = ptr
		runtime.SetFinalizer(cc.reader, readerFinalizer)
		cc.callback(cc.reader, nil)
	}
}

type readerAndCallback struct {
	reader   *reader
	conf     *C.pulsar_reader_configuration_t
	callback func(Reader, error)
}

func createReaderAsync(client *client, options ReaderOptions, callback func(Reader, error)) {
	if options.Topic == "" {
		callback(nil, errors.New("topic is required"))
		return
	}

	if options.StartMessageID == nil {
		callback(nil, errors.New("start message id is required"))
		return
	}

	reader := &reader{}

	if options.MessageChannel == nil {
		// If there is no message listener, set a default channel so that we can have receive to
		// use that
		reader.defaultChannel = make(chan ReaderMessage)
		options.MessageChannel = reader.defaultChannel
	}

	conf := C.pulsar_reader_configuration_create()

	C._pulsar_reader_configuration_set_reader_listener(conf, pointer.Save(&readerCallback{
		reader:  reader,
		channel: options.MessageChannel,
	}))

	if options.ReceiverQueueSize != 0 {
		C.pulsar_reader_configuration_set_receiver_queue_size(conf, C.int(options.ReceiverQueueSize))
	}

	if options.SubscriptionRolePrefix != "" {
		prefix := C.CString(options.SubscriptionRolePrefix)
		defer C.free(unsafe.Pointer(prefix))
		C.pulsar_reader_configuration_set_subscription_role_prefix(conf, prefix)
	}

	if options.Name != "" {
		name := C.CString(options.Name)
		defer C.free(unsafe.Pointer(name))

		C.pulsar_reader_configuration_set_reader_name(conf, name)
	}

	topic := C.CString(options.Topic)
	defer C.free(unsafe.Pointer(topic))

	C._pulsar_client_create_reader_async(client.ptr, topic, options.StartMessageID.(*messageID).ptr,
		conf, pointer.Save(&readerAndCallback{reader, conf, callback}))
}

type readerCallback struct {
	reader  Reader
	channel chan ReaderMessage
}

//export pulsarReaderListenerProxy
func pulsarReaderListenerProxy(cReader *C.pulsar_reader_t, message *C.pulsar_message_t, ctx unsafe.Pointer) {
	rc := pointer.Restore(ctx).(*readerCallback)
	rc.channel <- ReaderMessage{rc.reader, newMessageWrapper(message)}
}

func (r *reader) Topic() string {
	return C.GoString(C.pulsar_reader_get_topic(r.ptr))
}

func (r *reader) Next(ctx context.Context) (Message, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()

	case rm := <- r.defaultChannel:
		return rm.Message, nil
	}
}

func (r *reader) Close() error {
	channel := make(chan error)
	r.CloseAsync(func(err error) { channel <- err; close(channel) })
	return <-channel
}

func (r *reader) CloseAsync(callback func(error)) {
	if r.defaultChannel != nil {
		close(r.defaultChannel)
	}

	C._pulsar_reader_close_async(r.ptr, pointer.Save(callback))
}

//export pulsarReaderCloseCallbackProxy
func pulsarReaderCloseCallbackProxy(res C.pulsar_result, ctx unsafe.Pointer) {
	callback := pointer.Restore(ctx).(func(err error))

	if res != C.pulsar_result_Ok {
		callback(newError(res, "Failed to close Reader"))
	} else {
		callback(nil)
	}
}
