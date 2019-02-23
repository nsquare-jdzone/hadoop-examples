package com.javadeveloperzone;

import org.apache.hadoop.io.*;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class GeoLocationWritable implements WritableComparable<GeoLocationWritable> {

    private DoubleWritable longitude,latitude;
    private Text locationName;

    //default constructor for (de)serialization
    public GeoLocationWritable() {
        longitude = new DoubleWritable(0.0d);
        latitude = new DoubleWritable(0.0d);
        locationName = new Text();
    }

    public GeoLocationWritable(DoubleWritable longitude, DoubleWritable latitude, Text locationName) {
        this.longitude = longitude;
        this.latitude = latitude;
        this.locationName = locationName;
    }

    public void write(DataOutput dataOutput) throws IOException {
        longitude.write(dataOutput);
        latitude.write(dataOutput);
        locationName.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        longitude.readFields(dataInput);
        latitude.readFields(dataInput);
        locationName.readFields(dataInput);
    }

    public DoubleWritable getLongitude() {
        return longitude;
    }

    public void setLongitude(DoubleWritable longitude) {
        this.longitude = longitude;
    }

    public DoubleWritable getLatitude() {
        return latitude;
    }

    public void setLatitude(DoubleWritable latitude) {
        this.latitude = latitude;
    }

    public int compareTo(GeoLocationWritable o) {
        int dist = Double.compare(this.getLatitude().get(),o.getLatitude().get());
        dist= dist==0 ? Double.compare(this.getLongitude().get(),o.getLongitude().get()):dist;
        if(dist==0){
            return this.getLocationName().toString().compareTo(o.locationName.toString());
        }else{
            return dist;
        }
    }

    public void setLocationName(Text locationName) {
        this.locationName = locationName;
    }

    public Text getLocationName() {
        return locationName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GeoLocationWritable that = (GeoLocationWritable) o;
        return Objects.equals(longitude, that.longitude) &&
                Objects.equals(latitude, that.latitude) &&
                Objects.equals(locationName, that.locationName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(longitude, latitude, locationName);
    }
}
