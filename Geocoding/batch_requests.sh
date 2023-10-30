#!/bin/bash

for i in {1..300};
do
	curl --form addressFile=@input/input_file$i.0.csv --form benchmark=2020 https://geocoding.geo.census.gov/geocoder/locations/addressbatch --output output/output_file$i.csv
	echo Output file $i retrieved
done

