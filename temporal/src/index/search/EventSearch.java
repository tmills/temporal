package index.search;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Scanner;
import java.util.Set;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.QueryBuilder;

public class EventSearch {
	
	public static void main(String[] args) throws CorruptIndexException, IOException {
	
		String eventFile = "/home/dima/thyme/event-context/events.txt";
		Set<String> events = readEvents(eventFile);
		Set<String> notFoundEvents = verifyPresenseInIndex(events);
		System.out.println("total unique events: " + events.size());
		System.out.println("could not find in index: " + notFoundEvents.size());
		System.out.println(notFoundEvents);
	}
	
	public static Set<String> verifyPresenseInIndex(Set<String> events) throws CorruptIndexException, IOException {

		final int maxHits = 1;
		final String field = "content";

		Set<String> notFoundEvents = new HashSet<String>();
		
		DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get("/home/dima/data/mimic/index/")));
		IndexSearcher indexSearcher = new IndexSearcher(indexReader);
  	Analyzer standardAnalyzer = new StandardAnalyzer();
  	QueryBuilder queryBuilder = new QueryBuilder(standardAnalyzer);
  	
		for(String event : events) {
	  	Query query = queryBuilder.createPhraseQuery(field, event);
	  	
	  	TopDocs topDocs = indexSearcher.search(query, maxHits);
	  	ScoreDoc[] scoreDocs = topDocs.scoreDocs;

	  	if(scoreDocs.length < 1) {
	  		notFoundEvents.add(event);
	  	}
		}
		
		indexReader.close();
		return notFoundEvents;
	}
	
	public static Set<String> readEvents(String path) throws FileNotFoundException {
		
		Set<String> events = new HashSet<String>();
		try(Scanner scanner = new Scanner(new File(path))){
		  while(scanner.hasNextLine()) {
		    String line = scanner.nextLine();
		    String[] elements = line.split("\\|");
		    if(elements.length == 2 && elements[0].length() > 0) {
		      events.add(elements[0]);
		    }
		  }
		}
		return events;
	}
}

