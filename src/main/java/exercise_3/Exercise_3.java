package exercise_3;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.*;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.collection.Iterator;
import scala.collection.JavaConverters;
import scala.reflect.ClassTag$;
import scala.runtime.AbstractFunction1;
import scala.runtime.AbstractFunction2;
import scala.runtime.AbstractFunction3;
import shapeless.Tuple;

import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Exercise_3 {

    private static class VProg extends AbstractFunction3<Long,Tuple2<Integer, List<Long>>,Tuple2<Integer, List<Long>>,Tuple2<Integer, List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer, List<Long>> apply(Long vertexID, Tuple2<Integer, List<Long>> vertexValue, Tuple2<Integer, List<Long>> message) {
            System.out.println("VProg: " + vertexID + " " + vertexValue + " " + message);
            if (message._1 == Integer.MAX_VALUE) {             // superstep 0
                vertexValue._2.add(1l);
                return vertexValue;
            } else {                                        // superstep > 0
                //message._2.add(vertexID);
                return new Tuple2<>(Math.min(vertexValue._1,message._1), message._2);
            }
        }
    }

    private static class sendMsg extends AbstractFunction1<EdgeTriplet<Tuple2<Integer, List<Long>>,Integer>, Iterator<Tuple2<Object,Tuple2<Integer, List<Long>>>>> implements Serializable {
        @Override
        public Iterator<Tuple2<Object, Tuple2<Integer, List<Long>>>> apply(EdgeTriplet<Tuple2<Integer, List<Long>>, Integer> triplet) {
            Tuple2<Object,Tuple2<Integer, List<Long>>> sourceVertex = triplet.toTuple()._1();
            Tuple2<Object,Tuple2<Integer, List<Long>>> dstVertex = triplet.toTuple()._2();
            Integer distance = triplet.toTuple()._3();

            System.out.println("sendMSG: " + triplet);

            Integer sourceVertexPlusDistance = Integer.MAX_VALUE;
            if (Integer.parseInt(sourceVertex._2._1.toString()) != Integer.MAX_VALUE) {
                sourceVertexPlusDistance = Integer.parseInt(sourceVertex._2._1.toString()) + distance;
            }

            if (sourceVertexPlusDistance < dstVertex._2._1) {
                List<Long> newPath = sourceVertex._2._2;
                newPath.add(triplet.dstId());
                Tuple2<Integer, List<Long>> sendMessage = new Tuple2<Integer, List<Long>>(sourceVertexPlusDistance, sourceVertex._2._2);
                return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Integer, List<Long>>>(triplet.dstId(), sendMessage)).iterator()).asScala();
            }
            else return JavaConverters.asScalaIteratorConverter(Arrays.asList(new Tuple2<Object,Tuple2<Integer, List<Long>>>(triplet.dstId(), dstVertex._2)).iterator()).asScala();
        }
    }

    private static class merge extends AbstractFunction2<Tuple2<Integer, List<Long>>,Tuple2<Integer, List<Long>>,Tuple2<Integer, List<Long>>> implements Serializable {
        @Override
        public Tuple2<Integer, List<Long>> apply(Tuple2<Integer, List<Long>> o, Tuple2<Integer, List<Long>> o2) {
            System.out.println("merge: " + o + " " + o2);

            if (o._1 < o2._1) return o;
            else return o2;
        }
    }

    public static void shortestPathsExt(JavaSparkContext ctx) {
        Map<Long, String> labels = ImmutableMap.<Long, String>builder()
                .put(1l, "A")
                .put(2l, "B")
                .put(3l, "C")
                .put(4l, "D")
                .put(5l, "E")
                .put(6l, "F")
                .build();

        List<Tuple2<Object, Tuple2<Integer, List<Long>>>> vertices = Lists.newArrayList(
                new Tuple2<>(1l, new Tuple2<>(0, new LinkedList<>())),
                new Tuple2<>(2l, new Tuple2<>(Integer.MAX_VALUE, new LinkedList<>())),
                new Tuple2<>(3l, new Tuple2<>(Integer.MAX_VALUE, new LinkedList<>())),
                new Tuple2<>(4l, new Tuple2<>(Integer.MAX_VALUE, new LinkedList<>())),
                new Tuple2<>(5l, new Tuple2<>(Integer.MAX_VALUE, new LinkedList<>())),
                new Tuple2<>(6l, new Tuple2<>(Integer.MAX_VALUE, new LinkedList<>()))
        );
        List<Edge<Integer>> edges = Lists.newArrayList(
                new Edge<Integer>(1l, 2l, 4), // A --> B (4)
                new Edge<Integer>(1l, 3l, 2), // A --> C (2)
                new Edge<Integer>(2l, 3l, 5), // B --> C (5)
                new Edge<Integer>(2l, 4l, 10), // B --> D (10)
                new Edge<Integer>(3l, 5l, 3), // C --> E (3)
                new Edge<Integer>(5l, 4l, 4), // E --> D (4)
                new Edge<Integer>(4l, 6l, 11) // D --> F (11)
        );

        JavaRDD<Tuple2<Object, Tuple2<Integer, List<Long>>>> verticesRDD = ctx.parallelize(vertices);
        JavaRDD<Edge<Integer>> edgesRDD = ctx.parallelize(edges);

        Graph<Tuple2<Integer, List<Long>>, Integer> G = Graph.apply(verticesRDD.rdd(), edgesRDD.rdd(), null, StorageLevel.MEMORY_ONLY(), StorageLevel.MEMORY_ONLY(),
                scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        GraphOps ops = new GraphOps(G, scala.reflect.ClassTag$.MODULE$.apply(Tuple2.class), scala.reflect.ClassTag$.MODULE$.apply(Integer.class));

        LinkedList<Long> initialList = new LinkedList<>();
        initialList.add(1l);

        ops.pregel(new Tuple2<>(Integer.MAX_VALUE, initialList),
                        Integer.MAX_VALUE,
                        EdgeDirection.Out(),
                        new VProg(),
                        new sendMsg(),
                        new merge(),
                        ClassTag$.MODULE$.apply(Tuple2.class))
                .vertices()
                .toJavaRDD()
                .foreach(v -> {
                    Tuple2<Object, Tuple2<Integer, List<Long>>> vertex = (Tuple2<Object, Tuple2<Integer, List<Long>>>) v;
                    String path = "";
                    for (Long l: vertex._2._2) path += (labels.get(l) + " ");
                    System.out.println("Minimum cost to get from " + labels.get(1l) + " to " + labels.get(vertex._1) + " is " + vertex._2._1 + " with path: " + path);
                });
    }
	
}
