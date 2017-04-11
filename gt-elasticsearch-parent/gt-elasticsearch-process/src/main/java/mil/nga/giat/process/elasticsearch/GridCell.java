package mil.nga.giat.process.elasticsearch;

public class GridCell {
    private final String geohash;
    private final Number value;
    
    public GridCell(String geohash, Number value) {
        this.geohash = geohash;
        this.value = value;
    }
    
    public String getGeohash() {
        return geohash;
    }
    
    public Number getValue() {
        return value;
    }
}
