#include <cassert>

#include <span>

#include <seastar/core/app-template.hh>
#include <seastar/core/file.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/units.hh>
#include <seastar/core/seastar.hh>

static const size_t kElementSize = 4096;
static const size_t kAlignment = 4096;
static const size_t kMaxChunkSize = 256 * seastar::MB;
static_assert(kMaxChunkSize % kElementSize == 0);

struct Element {
  std::span<const std::byte, kElementSize> data_;

  Element(const std::byte *data) : data_{data, kElementSize} {}

  void assert_bytes_are_equal() {
    auto first = data_.front();
    for (auto b : data_) {
      assert(b == first);
    }
  }

  friend auto operator<=>(Element lhs, Element rhs) {
    auto cmp = memcmp(std::assume_aligned<kAlignment>(lhs.data_.data()),
                      std::assume_aligned<kAlignment>(rhs.data_.data()),
                      lhs.data_.size());
    return cmp;
  }

  friend auto operator==(Element lhs, Element rhs) {
    return lhs.data_.data() == rhs.data_.data();
  }
};

struct SortChunksJob {
  seastar::file in_;
  seastar::file out_;
  seastar::sstring file_name_;
  size_t chunk_size_;

  SortChunksJob(seastar::sstring file_name, size_t chunk_size)
      : file_name_{std::move(file_name)}, chunk_size_{chunk_size} {}

  seastar::future<> run() {
    size_t shard_id = seastar::this_shard_id();

    in_ = co_await seastar::open_file_dma(file_name_, seastar::open_flags::ro);

    size_t file_size = co_await in_.size();

    for (size_t offset = shard_id * chunk_size_, chunk_number = 0;
         offset < file_size;
         offset += seastar::smp::count * chunk_size_, chunk_number++) {

      const size_t this_chunk_size = std::min(chunk_size_, file_size - offset);
      assert(this_chunk_size % kElementSize == 0);

      auto chunk_buf = seastar::allocate_aligned_buffer<std::byte>(
          this_chunk_size, kAlignment);

      std::vector<Element> elements;
      elements.reserve(this_chunk_size / kElementSize);

      auto read_left = this_chunk_size;
      while (read_left) {
        auto read_already = this_chunk_size - read_left;
        auto try_read_this_much = std::min(
            read_left, in_.disk_read_max_length() / 16); // experimental value
        // std::cout << "shard_id = " << shard_id << " read_left = " << read_left
        //           << " read_already = " << read_already
        //           << " try_read_this_much = " << try_read_this_much << "\n";
        auto read_this_time = co_await in_.dma_read(
            offset + read_already, chunk_buf.get() + read_already,
            try_read_this_much);

        for (auto *buf = chunk_buf.get() + read_already,
                  *end = buf + read_this_time;
             buf != end; buf += kElementSize) {
          elements.emplace_back(buf);
        }

        read_left -= read_this_time;
      }

      // std::cout << "---------------shard_id = " << shard_id << "\n";

      //std::cout << "sorting in shard_id = " << shard_id << "\n";
      std::ranges::sort(elements);
      //std::cout << "finished sorting in shard_id = " << shard_id << "\n";

      auto file_name = seastar::format("chunk_{}_{}", shard_id, chunk_number);
      out_ = co_await seastar::open_file_dma(
          file_name, seastar::open_flags::wo | seastar::open_flags::create |
                         seastar::open_flags::truncate);

      co_await out_.truncate(this_chunk_size);

      // TODO: use scatter-gather I/O
      size_t current_offset = 0;
      for (auto elm : elements) {
        //elements.back().assert_bytes_are_equal();
        size_t written = co_await out_.dma_write(
            current_offset, elm.data_.data(), elm.data_.size());
        assert(written == elm.data_.size());
        current_offset += written;
      }

      co_await out_.flush();
      co_await out_.close();
    }

    co_await in_.close();
  }

  seastar::future<> stop() { return seastar::make_ready_future<>(); }
};

struct FileIterator {
  seastar::sstring file_name_;
  seastar::file in_;
  size_t file_size_;
  size_t current_offset_ = 0;
  std::unique_ptr<std::byte[], seastar::free_deleter> buf_ =
      seastar::allocate_aligned_buffer<std::byte>(kElementSize, kAlignment);

  FileIterator(seastar::sstring file_name) : file_name_{std::move(file_name)} {}

  seastar::future<> start() {
    in_ = co_await seastar::open_file_dma(file_name_, seastar::open_flags::ro);
    file_size_ = co_await in_.size();
    assert(file_size_ % kElementSize == 0);
    assert(file_size_ > 0);
  }

  seastar::future<bool> next() {
    if (current_offset_ >= file_size_)
      co_return false;

    auto read =
        co_await in_.dma_read(current_offset_, buf_.get(), kElementSize);
    assert(read);
    current_offset_ += kElementSize;

    co_return true;
  }

  seastar::future<> stop() {
    co_await in_.close();
    co_await seastar::remove_file(file_name_);
  }

  friend auto operator<=>(const FileIterator &lhs, const FileIterator &rhs) {
    auto cmp = memcmp(lhs.buf_.get(), rhs.buf_.get(), kElementSize);
    return cmp;
  }
};

int main(int argc, char **argv) {
  seastar::app_template app;

  app.add_options()(
      "file_name",
      boost::program_options::value<seastar::sstring>()->default_value(
          seastar::sstring("big_file")),
      "path to a file to sort");

  return app.run(argc, argv, [&]() -> seastar::future<> {
    auto &args = app.configuration();

    auto file_name = args["file_name"].as<seastar::sstring>();
    size_t file_size = co_await seastar::file_size(file_name);
    std::cout << "-------------file_size = " << file_size << "\n";
    size_t chunk_size =
        std::min((file_size - file_size % kElementSize) / seastar::smp::count,
                 kMaxChunkSize);
    assert(chunk_size % kElementSize == 0);
    std::cout << "-------------chunk_size = " << chunk_size << "\n";

    seastar::sharded<SortChunksJob> ctx;
    co_await ctx.start(file_name, chunk_size);
    co_await ctx.invoke_on_all([](SortChunksJob &pct) { return pct.run(); });
    co_await ctx.stop();

    // TODO: corner case - handle too many open files
    std::vector<FileIterator> in_files;
    size_t out_file_size = 0;
    for (int i = 0; i < seastar::smp::count; i++) {
      for (int j = 0;; j++) {
        auto file_name = seastar::format("chunk_{}_{}", i, j);

        if (co_await seastar::file_exists(file_name)) {
          in_files.push_back(std::move(file_name));
          co_await in_files.back().start();
          out_file_size += in_files.back().file_size_;
          auto res = co_await in_files.back().next();
          assert(res);
        } else {
          break;
        }
      }
    }

    auto out_ = co_await seastar::open_file_dma(
        "sorted_file", seastar::open_flags::wo | seastar::open_flags::create |
                           seastar::open_flags::truncate);

    std::cout << "out_file_size = " << out_file_size << "\n";
    co_await out_.truncate(out_file_size);
    size_t current_offset = 0;

    auto comp = [](const auto &lhs, const auto &rhs) { return lhs > rhs; };
    std::ranges::make_heap(in_files, comp);
    while (in_files.empty() == false) {
      std::ranges::pop_heap(in_files, comp);
      auto it = in_files.end();
      --it;
      auto written =
          co_await out_.dma_write(current_offset, it->buf_.get(), kElementSize);
      assert(written == kElementSize);
      current_offset += kElementSize;

      if (current_offset % seastar::GB == 0) {
        std::cout << "merged 1GB more\n";
      } else if (current_offset % (128 * seastar::MB) == 0) {
        std::cout << "merged 128MB more\n";
      }

      if (co_await it->next() == false) {
        co_await it->stop();
        in_files.erase(it);
      } else {
        std::ranges::push_heap(in_files, comp);
      }
    }

    co_await out_.flush();
    co_await out_.close();
  });
}
