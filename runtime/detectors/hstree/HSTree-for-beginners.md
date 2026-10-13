# HSTree for Beginners

This project offers **HSTree** with two variants: **Paper (Tan et al., 2011)**
(the default) and **PySAD**, selectable in the UI's Core parameters or through
`HSTREE_VARIANT`. The scoring explanation below describes the PySAD variant.
See [HSTree paper deviations](../../../docs/models/hstree/paper-deviations.md)
for the implementation differences.

## The idea in one sentence

A point is **suspicious** if it falls into an area where there are **very few other points**.

## A simple picture: a map divided into cells

Imagine a large square, the data space. It is divided into cells.

- If a cell contains **many** points, that is “normal”.
- If a cell contains **very few** points, that is “unusual”.

## How does HSTree build those cells?

HSTree builds several partitioning trees, like several city maps.
In each tree, the space is split in two, then split again, and so on.
This creates a hierarchy of cells: large cells -> smaller cells.

## How is a score assigned to a point?

1. The point follows the left/right splits all the way to the bottom of each tree.
2. At every level along that path, the algorithm counts how many reference points
   passed through the same region, giving more weight to counts at greater depths.
3. It adds these weighted counts along the path, then adds the result from every tree.
4. PySAD returns the negative of this sum: scores are at or below zero, and a score
   closer to zero means the point is more anomalous.

This is the PySAD implementation's scoring rule; it is not the paper's rule of
scoring only the terminal node.

The **Paper (Tan et al., 2011)** variant is selected by default, with
`HSTREE_VARIANT=paper`. It gives each tree its own randomly expanded work range,
then reads the reference mass from the first useful terminal node reached by the
point (maximum depth or a node with at most 10% of the reference window). The
PySAD variant remains available; the two variants can be selected for separate
campaigns and are identified in output records.

## Why does it work well in streaming?

HSTree can update its counters as data arrives.
So it gradually learns where the “usual” areas are.

## Adapting to changes

If your data changes a lot over time, for example if a new behavior becomes normal,
the algorithm must be able to “forget” part of the past. Otherwise, it will raise alerts too often.

## Mini example

You measure two things:

- height in centimeters
- speed in meters per second

Most points are around `(170 cm, 1 m/s)`.
A point `(170 cm, 10 m/s)` will usually pass through less populated regions, so
the summed score moves closer to zero and HSTree considers it **suspicious**.

---

**Keywords to remember:** cells, density, tree, rare area, anomaly.
