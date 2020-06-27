package it.polito.tdp.imdb.db;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import it.polito.tdp.imdb.model.Actor;
import it.polito.tdp.imdb.model.Adiacenza;
import it.polito.tdp.imdb.model.Director;
import it.polito.tdp.imdb.model.Movie;

public class ImdbDAO {
	
	public List<Actor> listAllActors(Map<Integer, Actor> idMap){
		String sql = "SELECT * FROM actors";
		List<Actor> result = new ArrayList<Actor>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Actor actor = new Actor(res.getInt("id"), res.getString("first_name"), res.getString("last_name"),
						res.getString("gender"));
				if(!idMap.containsKey(actor.getId())) {
					idMap.put(actor.getId(), actor);
				}
				
				result.add(actor);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<Movie> listAllMovies(){
		String sql = "SELECT * FROM movies";
		List<Movie> result = new ArrayList<Movie>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Movie movie = new Movie(res.getInt("id"), res.getString("name"), 
						res.getInt("year"), res.getDouble("rank"));
				
				result.add(movie);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	
	public List<Director> listAllDirectors(){
		String sql = "SELECT * FROM directors";
		List<Director> result = new ArrayList<Director>();
		Connection conn = DBConnect.getConnection();

		try {
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet res = st.executeQuery();
			while (res.next()) {

				Director director = new Director(res.getInt("id"), res.getString("first_name"), res.getString("last_name"));
				
				result.add(director);
			}
			conn.close();
			return result;
			
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public List<String> getGeneriFilm(){
		
		String sql = "SELECT DISTINCT genre FROM movies_genres";
		List<String> risultato = new ArrayList<>();
		
		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				risultato.add(rs.getString("genre"));
			}
			
			conn.close();
			return risultato;
			
		}catch(SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("ERRORE CARICAMENTO DATI DAL DATABASE");
		}
	}
	
	public List<Actor> getAttoriGenere(String genere, Map<Integer, Actor> idMap){
		
		String sql = "SELECT distinct actor_id AS id " + 
				"FROM roles AS r, movies_genres AS mg " + 
				"WHERE r.movie_id=mg.movie_id AND mg.genre =? ";
		List<Actor> risultato = new ArrayList<>();
		
		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, genere);
			
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				risultato.add(idMap.get(rs.getInt("id")));
			}
			
			conn.close();
			return risultato;
			
			
		}catch(SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("ERRORE CARICAMENTO DATI DAL DATABASE");
		}
	}
	
	public List<Adiacenza> getAdiacenze(String genere, Map<Integer, Actor> idMap){
		
		String sql = "SELECT distinct r1.actor_id AS attore1, r2.actor_id AS attore2, COUNT(*) AS peso " + 
				"FROM roles AS r1, roles AS r2, movies_genres AS mg " + 
				"WHERE mg.genre=? AND r1.actor_id>r2.actor_id " + 
				"AND mg.movie_id=r1.movie_id AND mg.movie_id=r2.movie_id " + 
				"GROUP BY r1.actor_id, r2.actor_id " + 
				"";
		
		List<Adiacenza> risultato = new ArrayList<>();
		
		try {
			Connection conn = DBConnect.getConnection();
			PreparedStatement st = conn.prepareStatement(sql);
			st.setString(1, genere);
			
			ResultSet rs = st.executeQuery();
			
			while(rs.next()) {
				Actor a1 = idMap.get(rs.getInt("attore1"));
				Actor a2 = idMap.get(rs.getInt("attore2"));
				risultato.add(new Adiacenza(a1, a2, rs.getInt("peso")));
			}
			
			conn.close();
			return risultato;
			
		}catch(SQLException e) {
			e.printStackTrace();
			throw new RuntimeException("ERRORE NEL CARICAMENTO DATI DAL DATABASE");
		}
	}
	
	
	
	
	
	
}
