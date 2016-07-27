Slide: http://www.slideshare.net/napo/mvp-osm
Paper: http://www.cs.nuim.ie/~pmooney/websitePapers/NapolitanoMooney_SOC_Bulletin_2012.pdf

MVP OSM: a tool to highlight high activity areas in OpenStreetMap based on detail level

By observing the world map offered by the OpenStreetMap community, it is relatively easy to realize which areas are higher in community activity and which are more marginal.
A high level of detail can certainly be considered as a good indicator of OSM data quality in a particular area.
The huge worldwide success of OpenStreetMap has been a magnet for further contributions by any kind of users... even those who don't have a GPS device and just import data retrieved from all sort of data sources.
This way, the map has become increasingly rich, but the concept of data quality is often questioned.

MVP OSM is a tool to highlight areas where the OSMers have dedicated attention to such details which strongly require the use of a GPS device, or at least a deep knowledge of the mapped area and a consequent desire to see it represented the best way possible.

The input file is a SpatiaLite (the GIS extension for SQLite) file derived from an OSM map file through spatialite_osm_raw.
This script processes the input file to create clusters, which are then used to derive single or multiple user activity on the area.
Vector geometries and heat maps can be plotted from the output file using a tool such as QGIS.

REQUIREMENTS:
* python >2.7
* spatialite >2.4
* spatialite-tool (spatialite_osm_raw)
* numpy

HOW TO USE
1. Export a small area from the OSM archives in the form of an .osm file
e.g.: http://geodati.fmach.it/gfoss_geodata/osm/output_osm_regioni/trentino-alto-adige.osm.bz2 (unpack it to get the osm file)

2. Convert the .osm file with spatialite_osm_raw
e.g.: spatialite_osm_raw -o trentino_alto_adige.osm -d trentino_alto_adige.sqlite

3. Launch the script
python mvp.py -i trentino_alto_adige.sqlite -o output.sqlite

4. Explore your data
e.g.: you could use QGIS Desktop and add output.sqlite as a SpatiaLite Layer.

NOTE
If you want to visualize your output as a heatmap, we suggest to

1. convert the .osm file into a SpatiaLite map first
e.g.: spatialite_osm_map -o trentino_alto_adige.osm -d trentino_alto_adige.map.sqlite

2. load both output.sqlite and trentino_alto_adige.map.sqlite as SpatiaLite Layers in QGIS

3. Drag and drop all layers from trentino_alto_adige.map.sqlite

4. Drag and drop layer "petlocations" from output.sqlite

5. Set layer transparency to 90% for "petlocations"

Enjoy!
