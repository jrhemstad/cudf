

class sync {
 public:
  sync(cudaStream_t s1, cudaStream_t s2) {
      cudaEventRecord(event, s1);
      cudaStreamWaitEvent(s2, event, 0);
  }

  ~sync() {
      cudaEventRecord(event, s2);
      cudaStreamwaitEvent(s1, event, 0);
  }
 private:
};