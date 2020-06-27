package it.polito.tdp.imdb.model;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jgrapht.Graphs;
import org.jgrapht.graph.DefaultWeightedEdge;
import org.jgrapht.graph.SimpleWeightedGraph;
import org.jgrapht.traverse.BreadthFirstIterator;

import it.polito.tdp.imdb.db.ImdbDAO;

public class Model {

	private ImdbDAO dao;
	private SimpleWeightedGraph<Actor, DefaultWeightedEdge> grafo;
	private Map<Integer, Actor> idMap;
	
	public Model() {
		
		this.dao = new ImdbDAO();
		this.idMap = new HashMap<>();
		this.dao.listAllActors(idMap);
	}
	
	public void creaGrafo(String genere) {
		
		this.grafo = new SimpleWeightedGraph<Actor, DefaultWeightedEdge>(DefaultWeightedEdge.class);
		List<Actor> vertici = this.dao.getAttoriGenere(genere, idMap);
		List<Adiacenza> archi = this.dao.getAdiacenze(genere, idMap);
		
		Graphs.addAllVertices(this.grafo, vertici);
		
		for(Adiacenza a : archi) {
			if(this.grafo.containsVertex(a.getAttore1()) && this.grafo.containsVertex(a.getAttore2())) {
				Graphs.addEdge(this.grafo, a.getAttore1(), a.getAttore2(), a.getPeso());
			}
		}
	}
	
	public int verticiSize() {
		return this.grafo.vertexSet().size();
	}
	
	public int archiSize() {
		return this.grafo.edgeSet().size();
	}
	
	public List<String> getGeneri(){
		return this.dao.getGeneriFilm();
	}
	
	public List<Actor> getAttoriGrafo(String genere){
		return this.dao.getAttoriGenere(genere, idMap);
	}
	
	public List<Actor> visitaGrafo(Actor sorgente){
		
		List<Actor> visita = new ArrayList<>();
		BreadthFirstIterator<Actor, DefaultWeightedEdge> bfv = new BreadthFirstIterator<Actor, DefaultWeightedEdge>(this.grafo, sorgente);
		bfv.next();
		while(bfv.hasNext()) {
			visita.add(bfv.next());
		}
		
		return visita;
	}
}
