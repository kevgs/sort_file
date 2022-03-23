#include <iostream>
#include <memory>
#include <random>
#include <string_view>

#include <seastar/core/aligned_buffer.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/loop.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/seastar.hh>
#include <seastar/core/units.hh>

static const std::string_view kFileName = "big_file";
static const size_t kChunkSize = 4096;
static const size_t kAlignment = 4096;
static const size_t kFileSize = 16 * seastar::GB;
static_assert(kFileSize % kChunkSize == 0);

struct PerCoreTask {
  seastar::file file_;
  std::unique_ptr<std::byte[], seastar::free_deleter> buf_ =
      seastar::allocate_aligned_buffer<std::byte>(kChunkSize, kAlignment);
  std::random_device rd_;
  std::default_random_engine dre_{rd_()};
  std::uniform_int_distribution<unsigned char> uid_{0, 9};

  seastar::future<> run() {
    file_ = co_await seastar::open_file_dma(kFileName, seastar::open_flags::wo);

    auto shard_id = seastar::this_shard_id();
    if (kFileSize / kChunkSize < shard_id) {
      co_await file_.close();
      co_return;
    }

    auto offsets = boost::irange(static_cast<uint64_t>(shard_id),
                                 kFileSize / kChunkSize, seastar::smp::count);
    co_await seastar::do_for_each(
        offsets, [this](auto offset) -> seastar::future<> {
          std::memset(std::assume_aligned<kAlignment>(buf_.get()), uid_(dre_),
                      kChunkSize);

          size_t written = co_await file_.dma_write(offset * kChunkSize,
                                                    buf_.get(), kChunkSize);
          if (written != kChunkSize)
            throw std::runtime_error(
                seastar::format("Only {} bytes were written instead of {}",
                                written, kChunkSize));
        });

    co_await file_.flush();
    co_await file_.close();
  }

  seastar::future<> stop() { return seastar::make_ready_future<>(); }
};

int main(int argc, char **argv) {
  seastar::app_template app;

  return app.run(argc, argv, [&]() -> seastar::future<> {
    auto file = co_await seastar::open_file_dma(
        kFileName, seastar::open_flags::wo | seastar::open_flags::create |
                       seastar::open_flags::truncate);
    co_await file.truncate(kFileSize);
    co_await file.close();

    seastar::sharded<PerCoreTask> ctx;
    co_await ctx.start();
    co_await ctx.invoke_on_all([](PerCoreTask &pct) { return pct.run(); });
    co_await ctx.stop();
  });
}
