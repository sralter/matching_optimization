# matching_optimization

By Samuel Alter

## Introduction

This project is designed to investigate how to improve the speed of a **polygon matching procedure**.

**The [results](#results) of the work yielded a 40x increase in the speed of the matching.**

The data is of building shapes in four municipal areas in Texas:

![Texas blobs with cluster bounding boxes](figs/blob_map_with_clusters.png)

The areas appear to be in four major cities in Texas: Dallas, Austin, Houston, and San Antonio.

We can zoom in on a sample of the polygons. Note that the blobs are very small, and their shapes are complex with many vertices. This will increase the storage requirements and thus retrieval times.

![Sample of blob shapes with scalebar](figs/blob_samples.png)

## Work Timeline

The flow of work for this project was as follows:
1. Isolate blob matching functions within `BlobSearchBusinesssClass.py`
2. Expand scope to supporting functions and scripts
3. Refocus with just the `BlobMatchingBusinessClass.py` script
4. Write benchmarking tools to measure and analyze performance of script
  * @Timer decorator
  * @ErrorCatcher decorator
  * Results analysis CLI script
5. Add logging messages and benchmarking hooks throughout script
6. Get performance results with limited, 835-row sample dataset
7. Get performance results with larger, ~6,000-row sample dataset
8. Swap `spawn` for `fork` in the `set_start_method` of multiprocessing:
  * `mp.set_start_method('fork', force=True)`
9. Determine which functions within script are least efficient

## Performance Results

Here are selections of the performance results of the smaller and larger sample datasets:

### Execution Time

![Execution time for smaller sample](figs/sample_smaller/execution_time_per_function.png)

![Execution time for larger sample](figs/sample_larger/execution_time_per_function.png)

### Function Calls over Time

![Function calls over time for smaller sample](figs/sample_smaller/function_calls_over_time.png)

![Function calls over time for larger sample](figs/sample_larger/function_calls_over_time.png)

### Memory Change per Function Call

![Memory change per function call for smaller sample](figs/sample_smaller/memory_change_per_function_call.png)

![Memory change per function call for larger sample](figs/sample_larger/memory_change_per_function_call.png)

### Select Histograms

![Match property between months for smaller sample](figs/sample_smaller/hist_9_match_properties_batched_perf_duration.png)

![Match property between months for larger sample](figs/sample_larger/hist_1_match_property_between_months_perf_duration.png)

![Match properties batched for smaller](figs/sample_smaller/hist_10_match_properties_batched_perf_duration.png)

![Match properties batched for karger](figs/sample_larger/hist_10_match_properties_batched_perf_duration.png)

### Top 10 Functions by Total Time

![Top 10 functions for smaller sample](figs/sample_smaller/top10_functions_by_total_time.png)

![Top 10 functions for larger sample](figs/sample_larger/top10_functions_by_total_time.png)

## Results <a name='results'></a>

Given the complexity of the script that we're trying to optimize, any change had effects to multiple other areas of the function. In the end, I realized that changing the way the multiprocessing handles child processes has a huge impact on the total execution time.

