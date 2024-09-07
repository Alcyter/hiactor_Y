#include "Timing.h"
#include <iostream>

std::chrono::high_resolution_clock::time_point  start;

void startTiming() {
    start = std::chrono::high_resolution_clock::now();
}

void stopTiming() {
    std::chrono::high_resolution_clock::time_point  end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
    std::cout << duration << std::endl;
}