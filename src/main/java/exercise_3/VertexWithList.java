package exercise_3;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class VertexWithList implements Serializable {
    private Integer distance;
    private List<Long> shortestPathList;

    public VertexWithList(Integer value) {
        this.distance = value;
        this.shortestPathList = new ArrayList<>();
    }

//    public VertexWithList(Integer value, List<Long> vertices) {
//        this.distance = value;
//        this.shortestPathList = vertices;
//    }

    public Integer getValue() {
        return distance;
    }

    public List<Long> getVertices() {
        return shortestPathList;
    }

    public void setValue(Integer value) {
        this.distance = value;
    }

    public void setVertices(List<Long> vertices) {
        this.shortestPathList = vertices;
    }

    public List<Long> addVertex(Long vertex) {
        this.shortestPathList.add(vertex);
        return this.shortestPathList;
    }

    @Override
    public String toString() {
        return "Vertex{" +
                "value=" + distance +
                ", vertices=" + shortestPathList +
                '}';
    }
}
