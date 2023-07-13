package org.Graph;

import org.GraphFileGen.GraphFileGen;
import org.GraphFileGen.PersonWritableComparable;
import org.apache.hadoop.io.WritableComparable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;

public class Graph {

    public static class Edge{
        Vertex src;
        Vertex dst;
        double weight;
        public Edge(Vertex f,Vertex t,double w){
            src=f;
            dst=t;
            weight=w;
        }
    }
    public static class Vertex{
        PersonWritableComparable person;
        ArrayList<Edge> inEdges;
        ArrayList<Edge> outEdges;

        ArrayList<PersonWritableComparable> predecessors;
        ArrayList<PersonWritableComparable> successors;
        public Vertex(PersonWritableComparable p){
            person=p;
            inEdges=new ArrayList<>();
            outEdges=new ArrayList<>();
            predecessors=new ArrayList<>();
            successors=new ArrayList<>();
        }
    }

    HashMap<PersonWritableComparable,Vertex> vertexOfPerson;

    public Graph(){
        vertexOfPerson=new HashMap<>();
    }

    private Vertex get_vertex_of_person(PersonWritableComparable person){
        Vertex v=vertexOfPerson.getOrDefault(person,null);
        if(v!=null)return v;
        Vertex newVertex=new Vertex(person);
        vertexOfPerson.put(person,newVertex);
        return newVertex;
    }
    public void add_edge(PersonWritableComparable from, PersonWritableComparable to,double w){
        Vertex src=get_vertex_of_person(from), dst=get_vertex_of_person(to);
        Edge newEdge=new Edge(src,dst,w);
        src.outEdges.add(newEdge);
        dst.inEdges.add(newEdge);
        src.successors.add(to);
        dst.predecessors.add(from);
    }
    public Collection<Edge> get_outEdges_of_person(PersonWritableComparable person){
        Vertex v=get_vertex_of_person(person);
        return v.outEdges;
    }
    public Collection<Edge> get_inEdges_of_person(PersonWritableComparable person){
        Vertex v=get_vertex_of_person(person);
        return v.inEdges;
    }
    public Collection<PersonWritableComparable> get_predecessors_of_person(PersonWritableComparable person){
        return get_vertex_of_person(person).predecessors;
    }
    public Collection<PersonWritableComparable> get_successors_of_person(PersonWritableComparable person){
        return get_vertex_of_person(person).successors;
    }

}
