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
      memcopy_threaded(dst, data, nbytes);
    } else {
        memcpy(dst, data, nbytes);
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


void memcopy_frame_aligned(uint8_t *dst, const uint8_t *src, uint64_t nbytes)
{
  struct timeval tv1, tv2;
  double elapsed = 0;
  // assume src and dst are ready to go (allocated, populated, etc)
  //printf("src=%p\tdst=%p\n", src, dst); 
  int rv = 0;
  int pagesz = getpagesize();
  char *srcbp = (char *)(((uint64_t)src + 4095) & ~(0x0fff));
  char *srcep = (char *)(((uint64_t)src + nbytes) & ~(0x0fff));
  uint64_t prefix = (uint64_t)srcbp - (uint64_t)src;
  uint64_t suffix = ((uint64_t)src + nbytes) % 4096;
  uint64_t numpages = (nbytes-prefix)/pagesz;
  char *dstep = (char *)((uint64_t)dst + prefix + numpages*pagesz);

  memcpy(dst, src, prefix);
  for (int64_t i = 0; i < numpages; i++)
  {
    memcpy((char *)(dst) + prefix + i*pagesz, ((char *)srcbp) + i*pagesz, pagesz);
  }
  memcpy(dstep, srcep, suffix);
  //return rv;
}

int memcopy_threaded(uint8_t *dst, const uint8_t *src, uint64_t nbytes) {
#ifndef NUMTHREADS
#define NUMTHREADS 8
#endif
  //struct timeval tv1, tv2;
  //double elapsed = 0;
  int rv = 0;
  std::vector<std::thread> threads;
  const uint64_t batchsz = nbytes/NUMTHREADS;
  // assume we can break up the copy evenly between available threads.
  // TODO(atumanov): deal with irregular sizes.
  if (nbytes % NUMTHREADS != 0 ) {
    // just a regular memcpy
    memcpy(dst, src, nbytes);
    return rv;
  }
//  gettimeofday(&tv1, NULL);
  for (int i = 0; i < NUMTHREADS; i++) {
//    threads.push_back(std::thread(memcopy_vanilla,
//                                  dst + i*batchsz,
//                                  src + i*batchsz, batchsz, false));
//    threads.push_back(std::thread(memcopy_frame_aligned,
//                                  dst + i*batchsz,
//                                  src + i*batchsz, batchsz));
    threads.push_back(std::thread(memcpy, dst + i*batchsz,src + i*batchsz,
                                  batchsz));
  }
  for (auto &t : threads) {
    t.join();
  }
//  gettimeofday(&tv2, NULL);
//  elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
//  printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
//         nbytes, elapsed, nbytes/((1<<20)*elapsed));
  //rv = validate(src,dst,nbytes);
  return rv;
}

};

}  // namespace numbuf

#endif  // PYNUMBUF_MEMORY_H
