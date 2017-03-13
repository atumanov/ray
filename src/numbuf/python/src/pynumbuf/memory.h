#ifndef PYNUMBUF_MEMORY_H
#define PYNUMBUF_MEMORY_H

#include <arrow/io/interfaces.h>

/* C++ includes */
#include <vector>
#include <string>
#include <thread>
using namespace std;

namespace numbuf {

class FixedBufferStream : public arrow::io::OutputStream,
                          public arrow::io::ReadableFileInterface {
 public:
  virtual ~FixedBufferStream() {}

  explicit FixedBufferStream(uint8_t* data, int64_t nbytes)
      : data_(data), position_(0), size_(nbytes) {}

  arrow::Status Read(int64_t nbytes, std::shared_ptr<arrow::Buffer>* out) override {
    DCHECK(out);
    DCHECK(position_ + nbytes <= size_) << "position: " << position_
                                        << " nbytes: " << nbytes << "size: " << size_;
    *out = std::make_shared<arrow::Buffer>(data_ + position_, nbytes);
    position_ += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status Read(int64_t nbytes, int64_t* bytes_read, uint8_t* out) override {
    assert(0);
    return arrow::Status::OK();
  }

  arrow::Status Seek(int64_t position) override {
    position_ = position;
    return arrow::Status::OK();
  }

  arrow::Status Close() override { return arrow::Status::OK(); }

  arrow::Status Tell(int64_t* position) override {
    *position = position_;
    return arrow::Status::OK();
  }

  arrow::Status Write(const uint8_t* data, int64_t nbytes) override {
    DCHECK(position_ >= 0 && position_ < size_);
    DCHECK(position_ + nbytes <= size_) << "position: " << position_
                                        << " nbytes: " << nbytes << "size: " << size_;
    uint8_t* dst = data_ + position_;
    if (nbytes >= (1<<20)) {
      memcopy_block_aligned(dst, data, nbytes, false);
    } else {
      memcopy_vanilla(dst, data, nbytes, false);
    }
    position_ += nbytes;
    return arrow::Status::OK();
  }

  arrow::Status GetSize(int64_t* size) override {
    *size = size_;
    return arrow::Status::OK();
  }

  bool supports_zero_copy() const override { return true; }

 private:
  uint8_t* data_;
  int64_t position_;
  int64_t size_;

void memcopy_vanilla(uint8_t *dst, const uint8_t *src, uint64_t nbytes, bool timeit) {
  struct timeval tv1, tv2;
  double elapsed = 0;

  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  memcpy(dst, src, nbytes);
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
  }
}

int memcopy_block_aligned(uint8_t *dst, const uint8_t *src, uint64_t nbytes, bool timeit)
{
#ifndef NUMTHREADS
#define NUMTHREADS 8
#endif
  int rv = 0;
  struct timeval tv1, tv2;
  double elapsed = 0;
  const uint64_t numthreads = NUMTHREADS;
  const uint64_t blocksz = getpagesize();
  const char *srcbp = (char *)(((uint64_t)src + blocksz-1) & ~(blocksz-1));
  char *srcep = (char *)(((uint64_t)src + nbytes) & ~(blocksz-1));
  const uint64_t numblocks = (((uint64_t)srcep - (uint64_t)srcbp)) / blocksz;
  // Now we divide these blocks between available threads. Remainder is pushed
  // to the suffix-handling thread.
  // uint64_t remainder = numblocks % numthreads;
  // Update the end pointer
  srcep = srcep - (numblocks % numthreads)*blocksz;
  const uint64_t chunksz = ((uint64_t)srcep - (uint64_t)srcbp) / numthreads;//B
  const uint64_t prefix = (uint64_t)srcbp - (uint64_t)src; // Bytes
  const uint64_t suffix = (uint64_t)(src+nbytes) - (uint64_t)srcep; // Bytes
  char *dstep = (char *)((uint64_t)dst + prefix + numthreads*chunksz);
  // Now data == | prefix | k*numthreads*blocksz | suffix |
  // chunksz = k*blocksz => data == | prefix | numthreads*chunksz | suffix |
  // Each thread gets a "chunk" of k blocks, except prefix and suffix threads.

  std::vector<std::thread> threads;
  // Start the prefix thread.
  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  threads.push_back(std::thread(memcpy, dst, src, prefix));
  for (int i = 1; i <= numthreads; i++) {
    threads.push_back(std::thread(
        memcpy, dst+prefix+(i-1)*chunksz, srcbp + (i-1)*chunksz, chunksz));
  }
  threads.push_back(std::thread(memcpy, dstep, srcep, suffix));

  // Join the memcpy threads.
  for (auto &t: threads) {
    t.join();
  }
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
  }
  return rv;
}

};

}  // namespace numbuf

#endif  // PYNUMBUF_MEMORY_H
