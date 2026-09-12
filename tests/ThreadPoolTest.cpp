#include "concurrency/ThreadPool.h"

#include <chrono>
#include <iostream>
#include <thread>

int main()
{
    forgedb::concurrency::ThreadPool pool(3);

    for (int i = 1; i <= 6; ++i) {
        pool.submit([i]() {
            std::cout << "Job " << i
                      << " running on thread "
                      << std::this_thread::get_id()
                      << std::endl;

            std::this_thread::sleep_for(
                std::chrono::milliseconds(500)
            );
        });
    }

    std::this_thread::sleep_for(
        std::chrono::seconds(2)
    );

    std::cout << "Test finished" << std::endl;

    return 0;
}