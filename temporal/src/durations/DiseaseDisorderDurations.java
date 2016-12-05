package durations;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.List;
import java.util.Set;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.FSDirectory;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.google.common.collect.Sets;

import utils.Utils;

public class DiseaseDisorderDurations {
	
	@SuppressWarnings("unchecked")
  public static void main(String[] args) throws CorruptIndexException, IOException {

    final String eventFile = args[0];
    Set<String> events = Utils.readSetValuesFromFile(eventFile);

    DirectoryReader indexReader = DirectoryReader.open(FSDirectory.open(Paths.get(args[1])));
    IndexSearcher indexSearcher = new IndexSearcher(indexReader);
	  	  
		Set<String> lessThanDaySuffixes = Sets.newHashSet("yesterday", "today", "overnight", "noted");
    Set<String> lessThanDayPrefixes = Sets.newHashSet("episode of", "presents with"); // admitted with?
		Set<String> moreThanDayPrefixes = Sets.newHashSet("history of", "diagnosed with");

		Set<List<String>> lessThanDayPatterns1 = Sets.cartesianProduct(events, lessThanDaySuffixes);
		Set<List<String>> lessThanDayPatterns2 = Sets.cartesianProduct(lessThanDayPrefixes, events);
		Set<List<String>> moreThanDayPatterns = Sets.cartesianProduct(moreThanDayPrefixes, events);
		
		Multiset<String> lessThanDayEvidence1 = countEvidence(indexSearcher, lessThanDayPatterns1, 0);
		Multiset<String> lessThanDayEvidence2 = countEvidence(indexSearcher, lessThanDayPatterns2, 1);
		Multiset<String> lessThanDayEvidence = Multisets.sum(lessThanDayEvidence1, lessThanDayEvidence2);
		Multiset<String> moreThanDayEvidence = countEvidence(indexSearcher, moreThanDayPatterns, 1);
		
		for(String event : Sets.union(lessThanDayEvidence.elementSet(), moreThanDayEvidence.elementSet())) {
		  System.out.format("%20s %4d %4d\n", event, lessThanDayEvidence.count(event), moreThanDayEvidence.count(event));
		}
		indexReader.close();
	}

	/**
	 * Count the number of hits for each pattern. Save the results in a multiset
	 * indexing on the specified element in the pattern.
	 * 
	 * @param patterns Set of patterns in which each list is a single pattern
	 * @param eventPosition The index of the event in each pattern
	 */
	public static Multiset<String> countEvidence(IndexSearcher indexSearcher, Set<List<String>> patterns, int eventPosition) throws IOException {

	  final int maxHits = 1000000;
	  final String searchField = "content";

	  Multiset<String> eventHitCounts = HashMultiset.create();
	  for(List<String> pattern : patterns) {
      
	    PhraseQuery.Builder queryBuilder = new PhraseQuery.Builder();
      queryBuilder.setSlop(0);
	    for(String string : pattern) {
	      String[] elements = string.split(" ");
	      for(String element : elements) {
	        queryBuilder.add(new Term(searchField, element));
	      }
	    }
	    
      TopDocs topDocs = indexSearcher.search(queryBuilder.build(), maxHits);
      ScoreDoc[] scoreDocs = topDocs.scoreDocs;     
      eventHitCounts.add(pattern.get(eventPosition), scoreDocs.length);
	  }
	  
	  return eventHitCounts;
	}
}

