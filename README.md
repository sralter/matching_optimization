# matching_optimization

By Samuel Alter

🚧 Under Construction 🚧

## Overview

This project serves as a demonstration of how geospatial-native approaches can improve a polygon matching process.

This project is divided into **two parts**:
1. Non-geospatial-native 

## Table of Contents <a name='toc'></a>

1. Under Construction...
2. Under Construction...
3. [Challenges and Solutions](#challenges)

## Challenges and Solutions <a name='challenges'></a>

* Matching was returning no results
  > I confirmed on QGIS that there were indeed overlaps, so I **reduced the resolution of the geohash** which allowed for a larger pre-filtering window.
  > I also made the polygons bigger to increase the chances that there will be an overlap.
* How to keep track of execution time, CPU and memory usage?
  > I used the `logging` library and add it as a decorator to the functions that I want to track.
* Multiprocessing with logging was creating log files for every batch
  > I edited the timing decorator function and wrapped the parallel matching function series in a larger function so that I could control the logging.
* Matching/Overlapping code does not find all true overlaps
  > Decrease the precision of the geohash prefiltering so that more candidates are included and not just exact matches. If the precision is too high, only exact matches will be included. 