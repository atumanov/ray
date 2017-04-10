#include <stdio.h>
#include <string.h>
#include <sys/time.h>
#include <stdint.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <assert.h>
//#include "memory.h"
#include <sys/mman.h>
#include <errno.h>

#include <vector>
#include <string>
#include <iostream>
#include <thread>

using namespace std;

#ifndef NUMTRIALS
#define NUMTRIALS 1
#endif

#ifndef NUMTHREADS
#define NUMTHREADS 8
#endif

#if 1
#define THREADPOOL_SIZE 8
#define MEMCOPY_BLOCK_SIZE 64
#define BYTES_IN_MB (1 << 20)

int create_buffer(int64_t size) {
  int fd;
#ifdef _WIN32
#else
#ifdef __linux__
  static char temp[] = "/dev/shm/plasmaXXXXXX";
#else
  static char temp[] = "/tmp/plasmaXXXXXX";
#endif
  char file_name[32];
  strncpy(file_name, temp, 32);
  fd = mkstemp(file_name);
  if (fd < 0)
    return -1;
  FILE *file = fdopen(fd, "a+");
  if (!file) {
    close(fd);
    return -1;
  }
  if (unlink(file_name) != 0) {
    fprintf(stderr, "unlink error");
    return -1;
  }
  if (ftruncate(fd, (off_t) size) != 0) {
    fprintf(stderr, "ftruncate error");
    return -1;
  }
#endif
  return fd;
}

void *do_mmap(size_t size) {
  int fd = create_buffer(size);
  printf("do_mmap: fd = %d\n", fd);
  errno = 0;
  //void *pointer = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE|MAP_POPULATE, fd, 0);
  void *pointer = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_SHARED|MAP_POPULATE, fd, 0);
  if (pointer == MAP_FAILED) {
   fprintf(stderr, "errno=%d\n", errno);
   perror("map failed");
   abort();
  } else {
    return pointer;
  }
}

class ParallelMemcopy {
 public:
  explicit ParallelMemcopy(uint64_t block_size, bool timeit, int threadpool_size)
      : timeit_(timeit),
        threadpool_(threadpool_size),
        block_size_(block_size),
        threadpool_size_(threadpool_size) {}

  void memcopy(uint8_t* dst, const uint8_t* src, uint64_t nbytes) {
    struct timeval tv1, tv2;
    if (timeit_) {
      // Start the timer.
      gettimeofday(&tv1, NULL);
    }
    if (nbytes >= BYTES_IN_MB) {
      memcopy_aligned(dst, src, nbytes, block_size_);
    } else {
      memcpy(dst, src, nbytes);
    }
    if (timeit_) {
      // Stop the timer and log the measured time.
      gettimeofday(&tv2, NULL);
      double elapsed =
          ((tv2.tv_sec - tv1.tv_sec) * 1000000 + (tv2.tv_usec - tv1.tv_usec)) / 1000000.0;
      // TODO: replace this with ARROW_LOG(ARROW_INFO) or better equivalent.
      printf("Copied %llu bytes in time = %8.4f MBps = %8.4f\n", nbytes, elapsed,
          nbytes / (BYTES_IN_MB * elapsed));
    }
  }

  ~ParallelMemcopy() {
    // Join threadpool threads just in case they are still running.
    for (auto& t : threadpool_) {
      if (t.joinable()) { t.join(); }
    }
  }

 private:
  /** Controls whether the memcopy operations are timed. */
  bool timeit_;
  /** Specifies the desired alignment in bytes, as a power of 2. */
  uint64_t block_size_;
  /** Number of threads to be used for parallel memcopy operations. */
  int threadpool_size_;
  /** Internal threadpool to be used in the fork/join pattern. */
  std::vector<std::thread> threadpool_;

  void memcopy_aligned(
      uint8_t* dst, const uint8_t* src, uint64_t nbytes, uint64_t block_size) {
    uint64_t num_threads = threadpool_size_;
    uint64_t dst_address = reinterpret_cast<uint64_t>(dst);
    uint64_t src_address = reinterpret_cast<uint64_t>(src);
    uint64_t left_address = (src_address + block_size - 1) & ~(block_size - 1);
    uint64_t right_address = (src_address + nbytes) & ~(block_size - 1);
    uint64_t num_blocks = (right_address - left_address) / block_size;
    // Update right address
    right_address = right_address - (num_blocks % num_threads) * block_size;
    // Now we divide these blocks between available threads. The remainder is
    // handled on the main thread.

    uint64_t chunk_size = (right_address - left_address) / num_threads;
    uint64_t prefix = left_address - src_address;
    uint64_t suffix = src_address + nbytes - right_address;
    // Now the data layout is | prefix | k * num_threads * block_size | suffix |.
    // We have chunk_size = k * block_size, therefore the data layout is
    // | prefix | num_threads * chunk_size | suffix |.
    // Each thread gets a "chunk" of k blocks.

    // Start all threads first and handle leftovers while threads run.
    for (int i = 0; i < num_threads; i++) {
      threadpool_[i] = std::thread(memcpy, dst + prefix + i * chunk_size,
          reinterpret_cast<uint8_t*>(left_address) + i * chunk_size, chunk_size);
    }

    memcpy(dst, src, prefix);
    memcpy(dst + prefix + num_threads * chunk_size,
        reinterpret_cast<uint8_t*>(right_address), suffix);

    for (auto& t : threadpool_) {
      if (t.joinable()) { t.join(); }
    }
  }
};
#endif

// declarations
int memcopy_frame_aligned(char *dst, const char *src, uint64_t nbytes,
                          uint64_t batch_size, bool runparallel, bool timeit);

int validate(const char *dst, const char *src, uint64_t nbytes) {
  int rv = memcmp(src, dst, nbytes);
  if (rv) {
    printf("memcopy was invalid, cmp failed at rv=%d\n", rv);
  }
  return rv; // 0 is ok, bad o.w.
}

int memcopy_vanilla(char *dst, const char *src, uint64_t nbytes, bool timeit) {
  struct timeval tv1, tv2;
  double elapsed = 0;
  int rv = 0;

  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  memcpy(dst, src, nbytes);
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
    rv = validate(src,dst,nbytes);
  }
  return rv;
}

int memcopy_threaded(char *dst, const char *src, uint64_t nbytes) {
  struct timeval tv1, tv2;
  double elapsed = 0;
  int rv = 0;
  std::vector<std::thread> threads;
  const uint64_t batchsz = nbytes/NUMTHREADS;
  // assume we can break up the copy evenly between available threads.
  assert(nbytes % NUMTHREADS == 0);
  gettimeofday(&tv1, NULL);
  for (int i = 0; i < NUMTHREADS; i++) {
    threads.push_back(std::thread(memcopy_vanilla,
                                  dst + i*batchsz,
                                  src + i*batchsz, batchsz, false));
//    threads.push_back(std::thread(memcopy_frame_aligned,
//                                  dst + i*batchsz,
//                                  src + i*batchsz, batchsz, 2, false, false));
//    threads.push_back(std::thread(memcpy, dst + i*batchsz,src + i*batchsz,
//                                  batchsz));
  }
  for (auto &t : threads) {
    t.join();
  }
  gettimeofday(&tv2, NULL);
  elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
  printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
         nbytes, elapsed, nbytes/((1<<20)*elapsed));
  rv = validate(src,dst,nbytes);
  return rv;
}

int memcopy_frame_aligned_unsafe(char *dst, const char *src, uint64_t nbytes) {
  //note that this assumes same page offset for src and dst
  //assert((uint64_t)src & 0xfff == (uint64_t)dst & 0xfff);
  struct timeval tv1, tv2;
  double elapsed;
  int pagesz = getpagesize();
  char *srcbp = (char *)(((uint64_t)src + 4095) & ~(0x0fff));
  char *dstbp = (char *)(((uint64_t)dst + 4095) & ~(0x0fff));
  char *dstep = (char *)(((uint64_t)dst + nbytes) & ~(0x0fff));
  char *srcep = (char *)(((uint64_t)src + nbytes) & ~(0x0fff));
  gettimeofday(&tv1, NULL);
  // fill in until next frame
  uint64_t prefix = (uint64_t)srcbp - (uint64_t)src;
  memcpy(dst, src, prefix);
  #pragma omp parallel for num_threads(NUMTHREADS)
  for (int64_t i = 0; i < (nbytes-prefix)/pagesz; i++)
  {
    memcpy(((char *)dstbp) + i*pagesz, ((char *)srcbp) + i*pagesz, pagesz);
  }
  #pragma omp barrier
  uint64_t suffix = ((uint64_t)dst + nbytes) % 4096;
  memcpy(dstep, srcep, suffix);
  gettimeofday(&tv2, NULL);
  elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
  printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
         nbytes, elapsed, nbytes/((1<<20)*elapsed));
  return validate(src, dst, nbytes);
}


int memcopy_frame(char *dst, const char *src, uint64_t nbytes,
                  uint64_t batch_size, bool runparallel, bool timeit) {
  struct timeval tv1, tv2;
  int rv = 0;
  double elapsed = 0;
  uint64_t pagesz = getpagesize();
  uint64_t blocksz = pagesz*batch_size;
  assert(nbytes%pagesz == 0); // allocating multiples of page frames
  int numpages = nbytes/pagesz;
  assert(numpages%batch_size == 0);

  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  #pragma omp parallel for num_threads(NUMTHREADS) if (runparallel)
  for (int64_t i = 0; i < numpages/batch_size; i++)
  {
    memcpy(dst + i * blocksz, src + i * blocksz, blocksz);
  }
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
    rv = validate(src, dst, nbytes);
  }
  return rv;
}

int memcopy_frame_parallel(char *dst, const char *src, uint64_t nbytes,
                           uint64_t batch_size, bool timeit) {
  struct timeval tv1, tv2;
  double elapsed = 0;
  int rv = 0;
  uint64_t pagesz = getpagesize();
  uint64_t blocksz = pagesz*batch_size;
  assert(nbytes%pagesz == 0); // allocating multiples of page frames
  int numpages = nbytes/pagesz;
  assert(numpages%batch_size == 0);

  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  #pragma omp parallel for num_threads(NUMTHREADS)
  for (int64_t i = 0; i < numpages/batch_size; i++)
  {
    memcpy(dst + i * blocksz, src + i * blocksz, blocksz);
  }
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
    rv = validate(src, dst, nbytes);
  }
  return rv;
}

int memcopy_frame_aligned(char *dst, const char *src, uint64_t nbytes,
                          uint64_t batch_size, bool runparallel, bool timeit)
{
  // High level idea: memcopy prefix + main aligned body + suffix
  // Prefix is everything up to the first aligned memory location
  // Suffix is after the last aligned memory location
  int rv = 0;
  struct timeval tv1, tv2;
  double elapsed = 0;
  uint64_t pagesz = getpagesize();
  uint64_t blocksz = pagesz*batch_size; // we'll aim for blocksz to match L1d cache size
  char *srcbp = (char *)(((uint64_t)src + blocksz-1) & ~(blocksz-1));//first aligned loc
  char *srcep = (char *)(((uint64_t)src + nbytes) & ~(blocksz-1));
  uint64_t prefix = (uint64_t)srcbp - (uint64_t)src;
  uint64_t suffix = ((uint64_t)src + nbytes) % blocksz;
  uint64_t numpages = (nbytes-prefix)/pagesz;
  uint64_t numblocks = numpages/batch_size;
  char *dstep = (char *)((uint64_t)dst + prefix + numblocks*blocksz);
 
  if (timeit) {
    gettimeofday(&tv1, NULL);
  }
  memcpy(dst, src, prefix);
  #pragma omp parallel for num_threads(NUMTHREADS) if (runparallel)
  for (int64_t i = 0; i < numblocks; i++)              
  {
    memcpy(dst + prefix + i * blocksz, srcbp + i * blocksz, blocksz);
  }
  memcpy(dstep, srcep, suffix);
  if (timeit) {
    gettimeofday(&tv2, NULL);
    elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
    printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
           nbytes, elapsed, nbytes/((1<<20)*elapsed));
    rv = validate(src,dst,nbytes);
  }
  return  rv;
}

int memcopy_coarse(char *dst, const char *src, uint64_t nbytes) {
  struct timeval tv1, tv2;
  double elapsed = 0;
  // Allocate NUMTHREADS coarse-grain threads and let each do
  assert(nbytes % NUMTHREADS == 0);
  uint64_t blocksz = nbytes/NUMTHREADS; // block size per thread
  gettimeofday(&tv1, NULL);
#pragma omp parallel for num_threads(NUMTHREADS)
  for (int t = 0; t < NUMTHREADS; t++) {
    //memcpy(dst + t*blocksz, src + t*blocksz, blocksz);
    memcopy_frame(dst + t*blocksz, src+t*blocksz, blocksz, 8, false, false);
  }
  gettimeofday(&tv2, NULL);
  elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
  printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
         nbytes, elapsed, nbytes/((1<<20)*elapsed));
  return validate(src, dst, nbytes);
}

uint64_t *alloc_randombytes(uint64_t nbytes) {

  srand48(time(NULL));
  uint64_t *src = NULL; //(uint64_t *) malloc(nbytes);
  src = reinterpret_cast<uint64_t *>(malloc(nbytes));
  //int rv = posix_memalign((void **)&src, 64, nbytes);
  // initialize src array with random data
  for (int i = 0 ; i < nbytes/sizeof(uint64_t); i++) {
    src[i] = lrand48();
  }

  return src;
}



int main(int argc, char **argv) {
  uint64_t NUMMB = 1UL;
  struct timeval tv1, tv2, tdiff;
  uint64_t *dst = NULL;
  uint64_t *src = NULL;
  int expnum = -1;
  double elapsed = 0;
  int pagesz = getpagesize();
  int rv;
  ParallelMemcopy memcopy_helper(MEMCOPY_BLOCK_SIZE, false, THREADPOOL_SIZE);


  if (argc < 2) {
    printf("USAGE: %s experiment_number [NUMMB]\n", argv[0]);
    exit(1);
  }

  expnum = atoi(argv[1]);
  if (argc >= 3) {
    // number of megabytes was specified
    NUMMB = atol(argv[2]);
  }
  const uint64_t nbytes = NUMMB * (1<<20);
  dst = reinterpret_cast<uint64_t *>(do_mmap(nbytes*2));
  //int rv = posix_memalign((void **)&dst, 64, nbytes);

  switch(expnum) {
    case -2:
    printf("ORIG C++ THREADS\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_threaded((char*)dst, (char*)src, nbytes);
    }
      break;

    case -1:
    printf("RAY C++ THREADS\n");
    for (int i=0; i<NUMTRIALS; i++) {
      struct timeval tv1, tv2;
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      gettimeofday(&tv1, NULL);
      memcopy_helper.memcopy(reinterpret_cast<uint8_t *>(dst),
          reinterpret_cast<uint8_t *>(src), nbytes);
      gettimeofday(&tv2, NULL);
      double elapsed = ((tv2.tv_sec - tv1.tv_sec)*1000000 + (tv2.tv_usec - tv1.tv_usec))/1000000.0;
      printf("copied %llu bytes in time = %8.4f MBps=%8.4f\n",
             nbytes, elapsed, nbytes/((1<<20)*elapsed));
    }
      break;

    // cases 0-3 : memcopy frame : omp=0/1, batching=0/1
    case 0:
    printf("MEMCOPY_FRAME;\tnoomp\tnobatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame((char*)dst, (char*)src, nbytes, 1, false, true);
    }
      break;

    case 1:
    printf("MEMCOPY_FRAME;\tnoomp\tbatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame((char*)dst, (char*)src, nbytes, 8, false, true);
    }
      break;

    case 2:
    printf("MEMCOPY_FRAME;\tomp\tnobatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame((char*)dst, (char*)src, nbytes, 1, true, true);
    }
      break;

    case 3:
    printf("MEMCOPY_FRAME;\tomp\tbatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame((char*)dst, (char*)src, nbytes, 8, true, true);
    }
      break;

// cases 4-7 are memcopy with frame alignment 
    case 4:
    printf("memcopy_frame_aligned:\tnoomp\tnobatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame_aligned((char*)dst, (char*)src, nbytes, 1, false, true);
    }
      break;

    case 5:
    printf("memcopy_frame_aligned:\tnoomp\tbatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame_aligned((char*)dst, (char*)src, nbytes, 8, false, true);
    }
      break;

    case 6:
    printf("memcopy_frame_aligned:\tomp\tnobatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame_aligned((char*)dst, (char*)src, nbytes, 1, true, true);
    }
      break;


    case 7:
    printf("memcopy_frame_aligned:\tomp\tbatch\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame_aligned((char*)dst, (char*)src, nbytes, 8, true, true);
    }
      break;

// the rest is miscellaneous
    case 8:
      //magic: no conditional on parallel for
      printf("memcopy_frame_parallel:\tomp\n");
    for (int i=0 ; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame_parallel((char*)dst, (char *)src, nbytes, 1, true);
    }
      break;

    case 9:
      printf("MEMCOPY_FRAME_ALIGNED unsafe\n");
    for (int i=0; i<NUMTRIALS; i++) {
      if (src) free(src);
      src = alloc_randombytes(nbytes);
      rv = memcopy_frame_aligned_unsafe((char*)dst, (char*)src, nbytes);
    }
      break;
    default:
      fprintf(stderr, "not implemented\n");
      exit(1);
  }

  return rv;
}

