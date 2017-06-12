/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef LIB_SHARED_BUFFER_H_
#define LIB_SHARED_BUFFER_H_

#include <boost/array.hpp>
#include <boost/asio.hpp>
#include <boost/make_shared.hpp>
#include <boost/shared_ptr.hpp>

#include <vector>

namespace pulsar {
namespace detail {

class SharedBufferInternal : public std::vector<char> {
 public:
    SharedBufferInternal(int size)
            : std::vector<char>(size) {
    }

    inline char* ptr() {
        return &(*this)[0];
    }

    void resize(int newSize) {
        std::vector<char>::resize(newSize);
    }

    int capacity() const {
        return std::vector<char>::capacity();
    }
};

}

class SharedBuffer {
 public:

    explicit SharedBuffer()
            : data_(),
              ptr_(0),
              readIdx_(0),
              writeIdx_(0),
              capacity_(0) {
    }

    /**
     * Allocate a buffer of given size
     */
    static SharedBuffer allocate(const uint32_t size) {
        return SharedBuffer(size);
    }

    /**
     * Create a buffer with a copy of memory pointed by ptr
     */
    static SharedBuffer copy(const char* ptr, uint32_t size) {
        SharedBuffer buf = allocate(size);
        buf.write(ptr, size);
        return buf;
    }

    static SharedBuffer copyFrom(const SharedBuffer& other, uint32_t capacity) {
        assert(other.readableBytes() <= capacity);
        SharedBuffer buf = allocate(capacity);
        buf.write(other.data(), other.readableBytes());
        return buf;
    }

    /**
     * Create a buffer that wraps the passed pointer, without copying the memory
     */
    static SharedBuffer wrap(char* ptr, size_t size) {
        return SharedBuffer(ptr, size);
    }

    inline const char* data() const {
        return ptr_ + readIdx_;
    }

    inline char* mutableData() {
        return ptr_ + writeIdx_;
    }

    /**
     * Return a shared buffer that include a portion of current buffer. No memory is copied
     */
    SharedBuffer slice(uint32_t offset) {
        SharedBuffer buf(*this);
        buf.consume(offset);
        return buf;
    }

    SharedBuffer slice(uint32_t offset, uint32_t length) {
        SharedBuffer buf(*this);
        buf.consume(offset);
        assert(buf.readableBytes() >= length);
        buf.writeIdx_ = buf.readIdx_ + length;
        return buf;
    }

    uint32_t readUnsignedInt() {
        assert(readableBytes() >= sizeof(uint32_t));
        uint32_t value = ntohl(*(uint32_t* ) data());
        consume(sizeof(uint32_t));
        return value;
    }

    uint16_t readUnsignedShort() {
        assert(readableBytes() >= sizeof(uint16_t));
        uint16_t value = ntohs(*(uint16_t* ) data());
        consume(sizeof(uint16_t));
        return value;
    }

    void writeUnsignedInt(uint32_t value) {
        assert(writableBytes() >= sizeof(uint32_t));
        *(uint32_t*) (mutableData()) = htonl(value);
        bytesWritten(sizeof(value));
    }

    void writeUnsignedShort(uint16_t value) {
        assert(writableBytes() >= sizeof(uint16_t));
        *(uint16_t*) (mutableData()) = htons(value);
        bytesWritten(sizeof(value));
    }

    inline uint32_t readableBytes() const {
        return writeIdx_ - readIdx_;
    }

    inline uint32_t writableBytes() const {
        return capacity_ - writeIdx_;
    }

    inline bool readable() const {
        return readableBytes() > 0;
    }

    inline bool writable() const {
        return writableBytes() > 0;
    }

    boost::asio::const_buffers_1 const_asio_buffer() const {
        return boost::asio::const_buffers_1(ptr_ + readIdx_, readableBytes());
    }

    boost::asio::mutable_buffers_1 asio_buffer() {
        assert(data_);
        return boost::asio::buffer(ptr_ + writeIdx_, writableBytes());
    }

    void write(const char* data, uint32_t size) {
        assert(size <= writableBytes());

        std::copy(data, data + size, mutableData());
        bytesWritten(size);
    }

    // Mark that some bytes were written into the buffer
    inline void bytesWritten(uint32_t size) {
        assert(size <= writableBytes());
        writeIdx_ += size;
    }

    // Return current writer index
    uint32_t writerIndex() {
        return writeIdx_;
    }

    // skip writerIndex
    void skipBytes(uint32_t size) {
        assert(writeIdx_ + size <= capacity_);
        writeIdx_ += size;
    }

    // set writerIndex
    void setWriterIndex(uint32_t index) {
        assert(index <= capacity_);
        writeIdx_ = index;
    }

    // Return current reader index
    uint32_t readerIndex() {
        return readIdx_;
    }

    // set readerIndex
    void setReaderIndex(uint32_t index) {
        assert(index <= capacity_);
        readIdx_ = index;
    }

    inline void consume(uint32_t size) {
        assert(size <= readableBytes());
        readIdx_ += size;
    }

    inline void rollback(uint32_t size) {
        assert(size <= readIdx_);
        readIdx_ -= size;
    }

    inline void reset() {
        readIdx_ = 0;
        writeIdx_ = 0;
    }

 private:

    typedef boost::shared_ptr<detail::SharedBufferInternal> BufferPtr;

    BufferPtr data_;
    char* ptr_;
    uint32_t readIdx_;
    uint32_t writeIdx_;
    uint32_t capacity_;

    SharedBuffer(char *ptr, size_t size)
            : data_(),
              ptr_(ptr),
              readIdx_(0),
              writeIdx_(size),
              capacity_(size) {
    }

    explicit SharedBuffer(size_t size)
            : data_(boost::make_shared<detail::SharedBufferInternal>(size)),
              ptr_(data_->ptr()),
              readIdx_(0),
              writeIdx_(0),
              capacity_(size) {
    }

};

template<int Size>
class CompositeSharedBuffer {
 public:

    void set(int idx, const SharedBuffer& buffer) {
        sharedBuffers_[idx] = buffer;
        asioBuffers_[idx] = buffer.const_asio_buffer();
    }

    // Implement the ConstBufferSequence requirements.
    typedef boost::asio::const_buffer value_type;
    typedef boost::asio::const_buffer* iterator;
    typedef const boost::asio::const_buffer* const_iterator;

    const boost::asio::const_buffer* begin() const {
        return &(asioBuffers_.at(0));
    }

    const boost::asio::const_buffer* end() const {
        return begin() + Size;
    }

 private:
    boost::array<SharedBuffer, Size> sharedBuffers_;
    boost::array<boost::asio::const_buffer, Size> asioBuffers_;
};

typedef CompositeSharedBuffer<2> PairSharedBuffer;

}

#endif /* LIB_SHARED_BUFFER_H_ */
