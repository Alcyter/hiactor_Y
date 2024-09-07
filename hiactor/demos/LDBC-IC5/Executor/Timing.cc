#include "Timing.h"
#include <iostream>

// std::chrono::high_resolution_clock::time_point  start;

// void startTiming() {
//     start = std::chrono::high_resolution_clock::now();
// }

// void stopTiming() {
//     std::chrono::high_resolution_clock::time_point  end = std::chrono::high_resolution_clock::now();
//     auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
//     std::cout << duration << std::endl;
// }

std::chrono::high_resolution_clock::time_point  start;
std::mutex time_mutex={};

void startTiming() {
    time_mutex.lock();
    if (start.time_since_epoch().count() == 0) {
        start = std::chrono::high_resolution_clock::now();
    }
    time_mutex.unlock();
}

void stopTiming() {
    std::chrono::high_resolution_clock::time_point  end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << duration << std::endl;
}