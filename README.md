# matching_optimization

By Samuel Alter

## Overview

I am experimenting on different approaches to improve polygon matching processing in a geospatial data context. 



This project is divided into **two parts**:
1. Non-geospatial-native 

## Table of Contents <a name='toc'></a>

1. 

## Challenges and Solutions <a name='challenges'></a>

* Matching was returning no results
  > I confirmed on QGIS that there were indeed overlaps, so I **reduced the resolution of the geohash** which allowed for a larger pre-filtering window.
* How to keep track of execution time, CPU and memory usage?
  > I used the `logging` library and add it as a decorator to the functions that I want to track.
* Multiprocessing with logging was creating log files for every batch
  > I edited the timing decorator function and wrapped the parallel matching function series in a larger function so that I could control the logging.

