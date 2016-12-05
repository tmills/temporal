package durations;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

public class MineCoarseDurations {

  public static void main(String[] args) throws IOException {
    if(args.length < 2){
      System.err.println("Two required arguments: <Event file (one per line)> <Lucene index directory>");
      System.exit(-1);
    }
    
    final String eventFile = args[0];
    final String indexDir = args[1];
    DirectoryReader ireader = DirectoryReader.open(FSDirectory.open(Paths.get(indexDir)));
    IndexSearcher indexSearcher = new IndexSearcher(ireader);

    Set<String> events = Utils.readSetValuesFromFile(eventFile);
    
    Set<String> ddShortPatterns = Sets.newHashSet("%s yesterday", "%s today", "%s overnight", "%s noted",
        "episode of %s", "presents with %s");
    Set<String> ddLongPatterns = Sets.newHashSet("history of %s", "diagnosed with %s");
    Set<String> shortVerbs = Sets.newHashSet("done", "performed");
    Set<String> shortTimes = Sets.newHashSet("yesterday", "today");
    Set<String> longVerbs = Sets.newHashSet("stopped", "started", "began", "finished", "ended");
    Set<String> longTimes = Sets.newHashSet("yesterday", "today", "last week", "last month");
    Set<String> procShortPatterns = new HashSet<>();
    for(String verb : shortVerbs){
      for(String time : shortTimes){
        procShortPatterns.add(verb + " %s " + time);
      }
    }
    
    Set<String> procLongPatterns = new HashSet<>();
    longVerbs.stream().forEach(verb -> {
      longTimes.stream().forEach(time -> {
        procLongPatterns.add("%s " + verb + " " + time);
      });
    });
    
    Multiset<String> shortEvidence = countEvidence(indexSearcher, events, Sets.union(ddShortPatterns, procShortPatterns));
    Multiset<String> longEvidence = countEvidence(indexSearcher, events, Sets.union(ddLongPatterns, procLongPatterns));
    
    for(String event : events) {
      System.out.format("%20s %4d %4d\n", event, shortEvidence.count(event), longEvidence.count(event));
    }
    ireader.close();

  }
  
  /**
   * Count the number of hits for each pattern. Save the results in a multiset
   * indexing on the specified element in the pattern.
   * 
   * @param patterns Set of patterns in which each list is a single pattern
   * @param eventPosition The index of the event in each pattern
   */
  public static Multiset<String> countEvidence(IndexSearcher indexSearcher, Set<String> events, Set<String> patterns) throws IOException {

    final int maxHits = 1000000;
    final String searchField = "content";

    Multiset<String> eventHitCounts = HashMultiset.create();
    
    for(String rawPattern : patterns) {
      for(String event : events) {
        
        String pattern = String.format(rawPattern, event);
        PhraseQuery.Builder builder = new PhraseQuery.Builder();
        builder.setSlop(0);
        for(String element : pattern.split(" ")) {
          builder.add(new Term(searchField, element));
        }

        PhraseQuery phraseQuery = builder.build();
        TopDocs topDocs = indexSearcher.search(phraseQuery, maxHits);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;     
        eventHitCounts.add(event, scoreDocs.length);
      }
    }
    
    return eventHitCounts;
  }
  
  public static Multiset<String> countEvidence(IndexSearcher indexSearcher, Map<String,List<PhraseQuery>> queries) throws IOException {

    final int maxHits = 1000000;
    final String searchField = "content";

    Multiset<String> eventHitCounts = HashMultiset.create();
    for(String event : queries.keySet()){
      for(PhraseQuery phraseQuery : queries.get(event)){
        TopDocs topDocs = indexSearcher.search(phraseQuery, maxHits);
        ScoreDoc[] scoreDocs = topDocs.scoreDocs;     
        eventHitCounts.add(event, scoreDocs.length);
      }
    }
    
    return eventHitCounts;
  }
}
